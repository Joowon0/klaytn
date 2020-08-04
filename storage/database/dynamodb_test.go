package database

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/klaytn/klaytn/log"
	"github.com/klaytn/klaytn/log/term"
	"github.com/mattn/go-colorable"

	"github.com/klaytn/klaytn/common"

	"github.com/stretchr/testify/assert"
)

func enableLog() {
	usecolor := term.IsTty(os.Stderr.Fd()) && os.Getenv("TERM") != "dumb"
	output := io.Writer(os.Stderr)
	if usecolor {
		output = colorable.NewColorableStderr()
	}
	glogger := log.NewGlogHandler(log.StreamHandler(output, log.TerminalFormat(usecolor)))
	log.PrintOrigins(true)
	log.ChangeGlobalLogLevel(glogger, log.Lvl(5))
	glogger.Vmodule("")
	glogger.BacktraceAt("")
	log.Root().SetHandler(glogger)
}

func TestDynamoDB(t *testing.T) {
	dynamo, err := NewDynamoDB(createTestDynamoDBConfig(), "aidan-test")
	defer dynamo.DeletedDB()
	if err != nil {
		t.Fatal(err)
	}
	//for i := 0; i < 100; i++ {
	testKey := common.MakeRandomBytes(10)
	testVal := common.MakeRandomBytes(500)

	val, err := dynamo.Get(testKey)
	//t.Log(val, err)
	assert.Nil(t, val)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), dataNotFoundErr.Error())

	assert.NoError(t, dynamo.Put(testKey, testVal))
	returnedVal, returnedErr := dynamo.Get(testKey)
	assert.Equal(t, testVal, returnedVal)
	assert.NoError(t, returnedErr)
	//}
}

func TestDynamoBatch(t *testing.T) {
	if testing.Verbose() {
		enableLog()
	}
	dynamo, err := NewDynamoDB(createTestDynamoDBConfig(), "aidan-dynamo-test")
	// defer dynamo.DeletedDB()
	if err != nil {
		fmt.Println(err)
		t.Fatal(err)
	}
	t.Log("dynamoDB", dynamo.config.TableName)

	// var testKeys [][]byte
	// var testVals [][]byte

	itemNum := 10000 * 120

	// testKeys := make([][]byte, itemNum)
	// testVals := make([][]byte, itemNum)

	batch := dynamo.NewBatch()
	defer batch.Close()

	start := time.Now()
	for i := 0; i < itemNum; i++ {
		if i%10000 == 0 {
			t.Log(i, time.Since(start))
		}
		testKey := common.MakeRandomBytes(32)
		testVal := common.MakeRandomBytes(500)

		// testKey := common.MakeRandomBytes(10)
		// testVal := common.MakeRandomBytes(50)
		//
		// testKeys = append(testKeys, testKey)
		// testVals = append(testVals, testVal)

		// assert.NoError(t, batch.Put(testKey, testVal))

		assert.NoError(t, batch.Put(testKey, testVal))

		// check if not exist
		// val, err := dynamo.Get(testKey)
		// assert.Nil(t, val)
		// assert.Error(t, err)
		// assert.Equal(t, err.Error(), dataNotFoundErr.Error())
	}

	assert.NoError(t, batch.Write())
	time.Sleep(10 * time.Second)
	//
	// // check if exist
	// for i := 0; i < itemNum; i++ {
	// 	returnedVal, returnedErr := dynamo.Get(testKeys[i])
	// 	assert.NoError(t, returnedErr)
	// 	assert.Equal(t, testVals[i], returnedVal)
	// }
}

func BenchmarkDynamoBatch_Write(b *testing.B) {
	dynamo, err := NewDynamoDB(createTestDynamoDBConfig(), "aidan-test")
	defer dynamo.DeletedDB()
	if err != nil {
		b.Fatal(err)
	}
	b.Log("dynamoDB", dynamo.config.TableName)

	var testKeys [][]byte
	var testVals [][]byte
	batch := dynamo.NewBatch()
	defer batch.Close()

	for j := 0; j < 100; j++ {
		itemNum := 1000
		for i := 0; i < itemNum; i++ {
			testKey := common.MakeRandomBytes(256)
			testVal := common.MakeRandomBytes(10)

			testKeys = append(testKeys, testKey)

			assert.NoError(b, batch.Put(testKey, testVal))
		}

		b.Log(batch.ValueSize() / 10)
		assert.NoError(b, batch.Write())
		time.Sleep(10 * time.Second)

		batch.Reset()
	}

	b.Log("Finish write")
	// check if exist
	for i := 0; i < len(testKeys); i++ {
		returnedVal, returnedErr := dynamo.Get(testKeys[i])
		assert.NoError(b, returnedErr)
		assert.Equal(b, testVals[i], returnedVal)
	}
}

func (dynamo *dynamoDB) DeletedDB() {
	dynamo.Close()
	dynamo.deleteTable()
	dynamo.fdb.deleteBucket()
}

// func TestDeleteBuckets(t *testing.T) {
// 	enableLog()
//
// 	lists := []string{
//
// 		"dynamo-test952987000-bucket",
// 		"dynamo-test982355000-bucket",
// 		"dynamo-test99746009-bucket",
// 	}
//
//
// 	session, err := session.NewSession(&aws.Config{
// 		Region:           aws.String("ap-northeast-2"),
// 		Endpoint:         aws.String("https://s3.ap-northeast-2.amazonaws.com"),
// 		S3ForcePathStyle: aws.Bool(true),
// 	})
//
// 	if err != nil {
// 		t.Fatal(err)
// 	}
//
// 	s3DB := &s3FileDB{
// 		s3:       s3.New(session),
// 	}
//
// 	for _, bucket := range lists {
// 		fmt.Println(bucket, s3DB)
// 		if _, err := s3DB.s3.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
// 			t.Fatal(err)
// 			// s3DB.logger.Error("failed to delete the test bucket", "err", err, "bucketName", s3DB.bucket)
// 		}
// 	}
// }
