package database

import (
	"testing"

	"github.com/klaytn/klaytn/common"

	"github.com/stretchr/testify/assert"
)

func TestDynamoDB(t *testing.T) {
	dynamo, err := NewDynamoDB(createTestDynamoDBConfig())
	if err != nil {
		t.Fatal(err)
	}
	//testRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	testKey := common.MakeRandomBytes(10)
	testVal := common.MakeRandomBytes(100)
	//testKey := randStrBytes(testRand.Intn(10))
	//testVal := randStrBytes(5 * 10)

	val, err := dynamo.Get(testKey)

	assert.Nil(t, val)
	assert.Error(t, err)
	//assert.True(t, strings.Contains(err.Error(), "NoSuchKey"))

	assert.NoError(t, dynamo.Put(testKey, testVal))

	returnedVal, returnedErr := dynamo.Get(testKey)
	assert.Equal(t, testVal, returnedVal)
	assert.NoError(t, returnedErr)
}
