package database

import (
	"testing"
	"time"

	"github.com/klaytn/klaytn/common"

	"github.com/stretchr/testify/assert"
)

func TestDynamoDB(t *testing.T) {
	dynamo, err := NewDynamoDB(createTestDynamoDBConfig())
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
	dynamo, err := NewDynamoDB(createTestDynamoDBConfig())
	t.Log("dynamoDB", dynamo.config.TableName)
	defer dynamo.DeletedDB()
	if err != nil {
		t.Fatal(err)
	}

	var testKeys [][]byte
	var testVals [][]byte
	batch := dynamo.NewBatch()

	for i := 0; i < 10; i++ {
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
	for i := 0; i < 10; i++ {
		returnedVal, returnedErr := dynamo.Get(testKeys[i])
		assert.NoError(t, returnedErr)
		assert.Equal(t, testVals[i], returnedVal)
	}
}

func (dynamo *dynamoDB) DeletedDB() {
	dynamo.deleteTable()
	dynamo.fdb.deleteBucket()
}
