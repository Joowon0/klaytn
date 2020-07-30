package database

import (
	"testing"
	"time"

	"github.com/klaytn/klaytn/common"

	"github.com/stretchr/testify/assert"
)

func TestDynamoDB(t *testing.T) {
	dynamo, err := NewDynamoDB(createTestDynamoDBConfig(), "winnie-test")
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

func TestDynamoDB_PutStream(t *testing.T) {
	dynamo, err := NewDynamoDB(createTestDynamoDBConfig(), "winnie-test")
	defer dynamo.DeletedDB()
	if err != nil {
		t.Fatal(err.Error())
	}

	itemNum := 100
	writeResultCh := make(chan error, 1)
	var testKeys [][]byte
	var testVals [][]byte

	for i := 0; i < itemNum; i++ {
		testKey := common.MakeRandomBytes(10)
		testVal := common.MakeRandomBytes(500)

		testKeys = append(testKeys, testKey)
		testVals = append(testVals, testVal)

		val, err := dynamo.Get(testKey)
		assert.Nil(t, val)
		assert.Error(t, err)
		assert.Equal(t, err.Error(), dataNotFoundErr.Error())

		dynamo.PutStream(testKey, testVal, writeResultCh)
	}

	for i := 0; i < itemNum; i++ {
		assert.NoError(t, <-writeResultCh)
		returnedVal, returnedErr := dynamo.Get(testKeys[i])
		assert.Equal(t, testVals[i], returnedVal)
		assert.NoError(t, returnedErr)
	}
}

func TestDynamoBatch(t *testing.T) {
	dynamo, err := NewDynamoDB(createTestDynamoDBConfig(), "winnie-test-noworker")
	defer dynamo.DeletedDB()
	if err != nil {
		t.Log(err.Error())
		t.Fatal(err)
	}
	t.Log("dynamoDB", dynamo.config.TableName)

	var testKeys [][]byte
	var testVals [][]byte
	batch := dynamo.NewBatch()
	defer batch.Close()

	itemNum := 5
	for i := 0; i < itemNum; i++ {
		testKey := common.MakeRandomBytes(10)
		testVal := common.MakeRandomBytes(50)

		testKeys = append(testKeys, testKey)
		testVals = append(testVals, testVal)

		assert.NoError(t, batch.Put(testKey, testVal))

		// check if not exist
		val, err := dynamo.Get(testKey)
		assert.Nil(t, val)
		assert.Error(t, err)
		assert.Equal(t, err.Error(), dataNotFoundErr.Error())
	}

	assert.NoError(t, batch.Write())
	time.Sleep(10 * time.Second)

	// check if exist
	for i := 0; i < itemNum; i++ {
		returnedVal, returnedErr := dynamo.Get(testKeys[i])
		assert.NoError(t, returnedErr)
		assert.Equal(t, testVals[i], returnedVal)
	}
}

func (dynamo *dynamoDB) DeletedDB() {
	dynamo.Close()
	dynamo.deleteTable()
	dynamo.fdb.deleteBucket()
}
