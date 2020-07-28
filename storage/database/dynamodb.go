package database

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

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
		ReadCapacityUnits:  100,
		WriteCapacityUnits: 100,
	}
}

type dynamoDB struct {
	config *DynamoDBConfig
	db     *dynamodb.DynamoDB
	fdb    fileDB
	logger log.Logger // Contextual logger tracking the database path

	// worker pool
	quitCh        chan struct{}
	writeCh       chan map[string]*dynamodb.AttributeValue
	writeResultCh chan error
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

	workerNum := runtime.NumCPU()

	db := dynamodb.New(session)
	dynamoDB := &dynamoDB{
		config: config,
		db:     db,
		logger: logger.NewWith("region", config.Region, "endPoint", config.Endpoint, "tableName", config.TableName),

		quitCh:        make(chan struct{}),
		writeCh:       make(chan map[string]*dynamodb.AttributeValue, workerNum*2),
		writeResultCh: make(chan error, workerNum*2),
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
				logger.Info("make a new dynamo batch write worker")
				go dynamoBatchWriteWorker(dynamoDB.db, aws.String(dynamoDB.config.TableName), dynamoDB.quitCh, dynamoDB.writeCh, dynamoDB.writeResultCh)
			}
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
		BillingMode: aws.String("PAY_PER_REQUEST"),
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
		//ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
		//	ReadCapacityUnits:  aws.Int64(dynamo.config.ReadCapacityUnits),
		//	WriteCapacityUnits: aws.Int64(dynamo.config.WriteCapacityUnits),
		//},
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
	//start := time.Now()
	//defer logger.Info("", "keySize", len(key), "valSize", len(val),
	//	 "putElapsedTime", time.Since(start))
	//return dynamo.internalBatch.Put(key, val)

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

	//getStart := time.Now()
	//defer func() {logger.Error("Get", "elapsed", time.Since(getStart))}()

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
	dynamoBatch := &dynamoBatch{db: dynamo, tableName: dynamo.config.TableName}

	return dynamoBatch
}

func (dynamo *dynamoDB) Meter(prefix string) {
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
	db                 *dynamoDB
	tableName          string
	batchItems         []map[string]*dynamodb.AttributeValue
	oversizeBatchItems []item
	size               int
}

func (batch *dynamoBatch) Put(key, val []byte) error {
	// If the size of the item is larger than the limit, it should be handled in different way
	if len(val) > dynamoWriteSizeLimit {
		batch.oversizeBatchItems = append(batch.oversizeBatchItems, item{key: key, val: val})
		batch.size += len(val)
		return nil
	}

	data := DynamoData{Key: key, Val: val}
	marshaledData, err := dynamodbattribute.MarshalMap(data)
	if err != nil {
		logger.Error("err while batch put", "err", err, "len(val)", len(val))
		return err
	}

	batch.batchItems = append(batch.batchItems, marshaledData)
	batch.size += len(val)
	return nil
}

func dynamoBatchWriteWorker(db *dynamodb.DynamoDB, tableName *string, quitChan <-chan struct{}, writeChan <-chan map[string]*dynamodb.AttributeValue, resultChan chan<- error) {
	for {
		select {
		case <-quitChan:
			return
		case item := <-writeChan:
			{
				params := &dynamodb.PutItemInput{
					TableName: tableName,
					Item:      item,
				}

				_, err := db.PutItem(params)
				if err != nil {
					resultChan <- err
				}
				resultChan <- nil
			}
		}
	}
}

func (batch *dynamoBatch) Write() error {
	//start := time.Now()
	var errs []error
	var overSizeErrChan chan error

	for _, item := range batch.batchItems {
		batch.db.writeCh <- item
	}

	go func(db *dynamoDB, overSizeErrChan chan error) {
		for _, item := range batch.oversizeBatchItems {
			_, err := db.fdb.write(item)
			if err == nil {
				if err2 := db.Put(item.key, overSizedDataPrefix); err2 != nil {
					//overSizeErrChan <- errors.Wrap(err2, "failed to put over size batch item in dynamo db")
				} else {
					//overSizeErrChan <- nil
				}
			} else {
				//overSizeErrChan <- errors.Wrap(err, "failed to put over size batch item in dynamo db")
			}
		}
	}(batch.db, overSizeErrChan)

	for range batch.batchItems {
		err := <-batch.db.writeResultCh
		if err != nil {
			errs = append(errs, err)
		}
	}

	//batch.db.logger.Info("write time", "elapsedTime", time.Since(start), "itemNum", len(batch.batchItems), "itemSize", batch.ValueSize())

	return nil
}

func (batch *dynamoBatch) ValueSize() int {
	return batch.size
}

func (batch *dynamoBatch) Reset() {
	batch.batchItems = []map[string]*dynamodb.AttributeValue{}
	batch.oversizeBatchItems = []item{}
	batch.size = 0
}

func (batch *dynamoBatch) Close() {
	//close(batch.quitCh)
}
