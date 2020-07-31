package database

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	metricutils "github.com/klaytn/klaytn/metrics/utils"
	"github.com/rcrowley/go-metrics"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/klaytn/klaytn/log"
)

var dataNotFoundErr = errors.New("data is not found with the given key")
var nilDynamoConfigErr = errors.New("attempt to create DynamoDB with nil configuration")

const dynamoReadSizeLimit = 400 * 1024
const dynamoWriteSizeLimit = 100 * 1024

const dynamoBatchWriteMaxCount = 5.0
const commitResultChSizeLimit = 500

// dynamo table provisioned setting
const ReadCapacityUnits = 10000
const WriteCapacityUnits = 10000

// batch write
const WorkerNum = 200
const itemChanSize = 500
const itemResultChanSize = WorkerNum * 2

type DynamoDBConfig struct {
	Region             string
	Endpoint           string
	TableName          string
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
}

/*
 * Please Run DynamoDB local with docker
 * $ docker pull amazon/dynamodb-local
 * $ docker run -d -p 8000:8000 amazon/dynamodb-local
 */
func createTestDynamoDBConfig() *DynamoDBConfig {
	return &DynamoDBConfig{
		Region:             "ap-northeast-2",
		Endpoint:           "https://dynamodb.ap-northeast-2.amazonaws.com", //"http://localhost:4569",  "https://dynamodb.ap-northeast-2.amazonaws.com"
		TableName:          "dynamo-test" + strconv.Itoa(time.Now().Nanosecond()),
		ReadCapacityUnits:  ReadCapacityUnits,
		WriteCapacityUnits: WriteCapacityUnits,
	}
}

type dynamoDB struct {
	config *DynamoDBConfig
	db     *dynamodb.DynamoDB
	fdb    fileDB
	logger log.Logger // Contextual logger tracking the database path

	// worker pool
	quitCh        chan struct{}
	writeCh       chan item
	writeResultCh chan error

	batchWriteTimeMeter       metrics.Meter
	batchWriteCountMeter      metrics.Meter
	batchWriteSizeMeter       metrics.Meter
	batchWriteSecPerItemMeter metrics.Meter
	batchWriteSecPerByteMeter metrics.Meter
}

func NewDynamoDB(config *DynamoDBConfig, tableName string) (*dynamoDB, error) {
	if config == nil {
		return nil, nilDynamoConfigErr
	}

	if len(tableName) != 0 {
		config.TableName = tableName
	}

	session := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Endpoint: aws.String(config.Endpoint),
			Region:   aws.String(config.Region),
		},
	}))

	workerNum := WorkerNum

	db := dynamodb.New(session)
	dynamoDB := &dynamoDB{
		config: config,
		db:     db,
		logger: logger.NewWith("region", config.Region, "endPoint", config.Endpoint, "tableName", config.TableName),

		quitCh:        make(chan struct{}),
		writeCh:       make(chan item, itemChanSize),
		writeResultCh: make(chan error, itemResultChanSize),
	}

	logger.Info("creating s3FileDB ", config.TableName+"-bucket")
	s3FileDB, err := newS3FileDB(config.Region, "https://s3.ap-northeast-2.amazonaws.com", config.TableName+"-bucket")
	if err != nil {
		dynamoDB.logger.Error("Unable to create/get S3FileDB", "DB", config.TableName+"-bucket")
		return nil, err
	}
	dynamoDB.fdb = s3FileDB

	// Check if the table is ready to serve
	for {
		tableStatus, err := dynamoDB.tableStatus()
		if err != nil {
			if strings.Contains(err.Error(), "ResourceNotFoundException") {
				if err := dynamoDB.createTable(); err != nil {
					dynamoDB.logger.Crit("unable to create dynamo table", "err", err.Error())
					return nil, err
				}
			} else {
				dynamoDB.logger.Crit("unable to get dynamo table status", "err", err.Error())
				return nil, err
			}
		}

		switch tableStatus {
		case dynamodb.TableStatusActive:
			dynamoDB.logger.Info("DynamoDB configurations")
			for i := 0; i < workerNum; i++ {
				go dynamoBatchWriteWorker(dynamoDB, dynamoDB.quitCh, dynamoDB.writeCh, dynamoDB.writeResultCh)
			}
			logger.Info("made new dynamo batch write workers", "workerNum", workerNum)
			return dynamoDB, nil
		case dynamodb.TableStatusDeleting, dynamodb.TableStatusArchiving, dynamodb.TableStatusArchived:
			return nil, errors.New("failed to get DynamoDB table, table status : " + tableStatus)
		default:
			time.Sleep(1 * time.Second)
			dynamoDB.logger.Info("waiting for the table to be ready", "table status", tableStatus)
		}
	}
}

func (dynamo *dynamoDB) createTable() error {
	input := &dynamodb.CreateTableInput{
		BillingMode: aws.String("PROVISIONED"),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Key"),
				AttributeType: aws.String("B"), // B - the attribute is of type Binary
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Key"),
				KeyType:       aws.String("HASH"), // HASH - partition key, RANGE - sort key
			},
			//{
			//	AttributeName: aws.String("Title"),
			//	KeyType:       aws.String("RANGE"),
			//},
		},
		// TODO change ProvisionedThroughput according to node status
		//      check dynamodb.updateTable
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(dynamo.config.ReadCapacityUnits),
			WriteCapacityUnits: aws.Int64(dynamo.config.WriteCapacityUnits),
		},
		TableName: aws.String(dynamo.config.TableName),
	}

	_, err := dynamo.db.CreateTable(input)
	if err != nil {
		dynamo.logger.Error("Error while creating the DynamoDB table", "err", err, "tableName", dynamo.config.TableName)
		return err
	}
	dynamo.logger.Info("Successfully created the Dynamo table", "tableName", dynamo.config.TableName)
	return nil
}

func (dynamo *dynamoDB) deleteTable() error {
	if _, err := dynamo.db.DeleteTable(&dynamodb.DeleteTableInput{TableName: &dynamo.config.TableName}); err != nil {
		dynamo.logger.Error("Error while deleting the DynamoDB table", "tableName", dynamo.config.TableName)
		return err
	}
	dynamo.logger.Info("Successfully deleted the DynamoDB table", "tableName", dynamo.config.TableName)
	return nil
}

func (dynamo *dynamoDB) tableStatus() (string, error) {
	desc, err := dynamo.tableDescription()
	if err != nil {
		return "", err
	}

	return *desc.TableStatus, nil
}

func (dynamo *dynamoDB) tableDescription() (*dynamodb.TableDescription, error) {
	describe, err := dynamo.db.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(dynamo.config.TableName)})
	if describe == nil {
		return nil, err
	}

	return describe.Table, err
}

func (dynamo *dynamoDB) Type() DBType {
	return DynamoDB
}

// Path returns the path to the database directory.
func (dynamo *dynamoDB) Path() string {
	return fmt.Sprintf("%s-%s", dynamo.config.Region, dynamo.config.Endpoint)
}

type DynamoData struct {
	Key []byte `json:"Key" dynamodbav:"Key"`
	Val []byte `json:"Val" dynamodbav:"Val"`
}

// Put inserts the given key and value pair to the database.
func (dynamo *dynamoDB) Put(key []byte, val []byte) error {
	if len(key) == 0 {
		return nil
	}

	if len(val) > dynamoWriteSizeLimit {
		_, err := dynamo.fdb.write(item{key: key, val: val})
		if err != nil {
			return err
		}

		return dynamo.Put(key, overSizedDataPrefix)
	}

	data := DynamoData{Key: key, Val: val}
	marshaledData, err := dynamodbattribute.MarshalMap(data)
	if err != nil {
		return err
	}

	params := &dynamodb.PutItemInput{
		TableName: aws.String(dynamo.config.TableName),
		Item:      marshaledData,
	}

	//marshalTime := time.Since(start)
	_, err = dynamo.db.PutItem(params)
	if err != nil {
		fmt.Printf("Put ERROR: %v\n", err.Error())
		return err
	}

	return nil
}

// Has returns true if the corresponding value to the given key exists.
func (dynamo *dynamoDB) Has(key []byte) (bool, error) {
	if _, err := dynamo.Get(key); err != nil {
		return false, err
	}
	return true, nil
}

var overSizedDataPrefix = []byte("oversizeditem")

// Get returns the corresponding value to the given key if exists.
func (dynamo *dynamoDB) Get(key []byte) ([]byte, error) {
	params := &dynamodb.GetItemInput{
		TableName: aws.String(dynamo.config.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				B: key,
			},
		},
		ConsistentRead: aws.Bool(true),
	}

	result, err := dynamo.db.GetItem(params)
	if err != nil {
		fmt.Printf("Get ERROR: %v\n", err.Error())
		return nil, err
	}

	if result.Item == nil {
		return nil, dataNotFoundErr
		//return dynamo.fdb.read(key)
	}

	var data DynamoData
	if err := dynamodbattribute.UnmarshalMap(result.Item, &data); err != nil {
		return nil, err
	}

	if data.Val == nil {
		return nil, dataNotFoundErr
	}

	if !bytes.Equal(data.Val, overSizedDataPrefix) {
		return data.Val, nil
	} else {
		return dynamo.fdb.read(key)
	}
}

// Delete deletes the key from the queue and database
func (dynamo *dynamoDB) Delete(key []byte) error {
	params := &dynamodb.DeleteItemInput{
		TableName: aws.String(dynamo.config.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				B: key,
			},
		},
	}

	result, err := dynamo.db.DeleteItem(params)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err.Error())
		return err
	}
	fmt.Println(result)
	return nil
}

func (dynamo *dynamoDB) Close() {
	close(dynamo.quitCh)
}

func (dynamo *dynamoDB) NewBatch() Batch {
	dynamoBatch := &dynamoBatch{db: dynamo, tableName: dynamo.config.TableName, start: time.Now()}

	return dynamoBatch
}

func (dynamo *dynamoDB) Meter(prefix string) {
	dynamo.batchWriteTimeMeter = metrics.NewRegisteredMeter(prefix+"batchwrite/time", nil)
	dynamo.batchWriteCountMeter = metrics.NewRegisteredMeter(prefix+"batchwrite/count", nil)
	dynamo.batchWriteSizeMeter = metrics.NewRegisteredMeter(prefix+"batchwrite/size", nil)
	dynamo.batchWriteSecPerItemMeter = metrics.NewRegisteredMeter(prefix+"batchwrite/secperitem", nil)
	dynamo.batchWriteSecPerByteMeter = metrics.NewRegisteredMeter(prefix+"batchwrite/secperbyte", nil)
}

func (dynamo *dynamoDB) NewIterator() Iterator {
	return nil
}

func (dynamo *dynamoDB) NewIteratorWithStart(start []byte) Iterator {
	return nil
}

func (dynamo *dynamoDB) NewIteratorWithPrefix(prefix []byte) Iterator {
	return nil
}

type dynamoBatch struct {
	db        *dynamoDB
	tableName string

	count     int
	size      int
	leftCount int

	start time.Time
}

func (batch *dynamoBatch) Put(key, val []byte) error {
	if len(key) == 0 {
		return nil
	}

	batch.count++
	batch.size += len(val)

	go func() {
		batch.db.writeCh <- item{key: key, val: val}
	}()

	return nil
}

func dynamoBatchWriteWorker(db *dynamoDB, quitChan <-chan struct{}, writeChan <-chan item, resultChan chan<- error) {
	for {
		select {
		case <-quitChan:
			return
		case item := <-writeChan:
			resultChan <- db.Put(item.key, item.val)
		}
	}
}

func (batch *dynamoBatch) Write() error {
	if batch.count == 0 {
		return nil
	}

	var err error
	for i := 0; i < batch.count; i++ {
		err2 := <-batch.db.writeResultCh
		if err2 != nil {
			err = err2
			logger.Error("failed to batch put", "err", err2, "count", i)
		}
	}

	elapsed := time.Since(batch.start)
	batch.db.logger.Debug("write time", "elapsedTime", elapsed, "itemNum", (batch.count), "itemSize", batch.ValueSize())
	if metricutils.Enabled {
		batch.db.batchWriteTimeMeter.Mark(int64(elapsed.Seconds()))
		batch.db.batchWriteCountMeter.Mark(int64(batch.count))
		batch.db.batchWriteSizeMeter.Mark(int64(batch.size))
		batch.db.batchWriteSecPerItemMeter.Mark(int64(int(elapsed.Seconds()) / batch.count))
		batch.db.batchWriteSecPerByteMeter.Mark(int64(int(elapsed.Seconds()) / batch.size))
	}

	if err != nil {
		return err
	}
	return nil
}

func (batch *dynamoBatch) ValueSize() int {
	return batch.size
}

func (batch *dynamoBatch) Reset() {
	batch.size = 0
	batch.count = 0
	batch.leftCount = 0
}

func (batch *dynamoBatch) Close() {
}