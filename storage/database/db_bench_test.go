package database

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/klaytn/klaytn/common"
)

type entry struct {
	key []byte
	val []byte
}

var workspace = "/home/ubuntu/klaytn/benchmark_data"
var ldbWorkSpace = workspace + "/ldb"

// make data file
func TestCreateEntries(b *testing.T) {
	fileName := fmt.Sprintf("%s/entries.txt", workspace)
	b.Log(fileName)
	fo, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()

	keys := common.MakeRandomBytesSlice(256,entryNum)
	values := common.MakeRandomBytesSlice(600,entryNum)	
	for i := 0; i < entryNum; i++ {
		_, err := fo.Write(keys[i]) // key
		if err != nil {
		 b.Log("err: ", err)
		}
		_, err = fo.Write(values[i]) // value
		if err != nil {
			b.Log("err: ", err)
		 }
	}
}

// read data file
func GetEntries(b *testing.T, n int) []entry {
	fileName := fmt.Sprintf("%s/entries.txt", workspace)
	fi, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	// close fi on exit and check for its returned error
	defer func() {
		if err := fi.Close(); err != nil {
			b.Log("error while closing file")
			panic(err)
		}
	}()

	randomEntries := make([]entry, n)
	keyBuff := make([]byte, 256)
	valBuff := make([]byte, 600)
	for i := 0; i < n; i++ {
		n, err := fi.Read(keyBuff)
		if err != nil && err != io.EOF && n != 256 {
			b.Log("failed to get key from file")
			assert.FailNow(b, err.Error())
		}
		n, err = fi.Read(valBuff)
		if err != nil && err != io.EOF && n != 600 {
			b.Log("failed to get value from file")
			assert.FailNow(b, err.Error())
		}
		randomEntries[i].key = make([]byte, 256)
		randomEntries[i].val = make([]byte, 600)
		copy(randomEntries[i].key, keyBuff[:])
		copy(randomEntries[i].val, valBuff[:])
	}

	return randomEntries
}

const entryNum = 100

func Test_LevelRead(b *testing.T) {
	benchDB(b, LevelDB, "get")
}
func Test_LevelWrite(b *testing.T) {
	benchDB(b, LevelDB, "put")
}
func Test_LevelBatchWrite(b *testing.T) {
	benchDB(b, LevelDB, "batchWrite")
}
func Test_DynamoRead(b *testing.T) {
	benchDB(b, DynamoDB, "get")
}
func Test_DynamoWrite(b *testing.T) {
	benchDB(b, DynamoDB, "put")
}

func Test_DynamoBatchWrite(b *testing.T) {
	benchDB(b, DynamoDB, "batchWrite")
}

func benchDB(b *testing.T, dbType DBType, testType string) {
	dbc := &DBConfig{Dir: ldbWorkSpace + strconv.Itoa(entryNum), DBType: dbType, SingleDB: true, LevelDBCacheSize: 128, OpenFilesLimit: 128,
		DynamoDBConfig: GetDefaultDynamoDBConfig()}
	dbm := NewDBManager(dbc)
	db := dbm.GetStateTrieDB()
	defer dbm.Close()

	// set function
	var f func(key, value []byte, batch Batch) error
	if testType == "put" {
		f = func(key, value []byte, batch Batch) error {
			return db.Put(key, value)
		}
	} else if testType == "batchWrite" {
		f = func(key, value []byte, batch Batch) error {
			batch.Put(key, value)
			return nil
		}
	} else if testType == "get" {
		f = func(key, value []byte, batch Batch) error {
			val, err := db.Get(key)
			if err != nil || len(val) == 0 {
				return err
			}

			assert.Equal(b, value, val)
			return err
		}
	} else {
		f = func(key, value []byte, batch Batch) error {
			b.FailNow()
			return errors.New("not correct test type")
		}
	}

	entries := GetEntries(b, entryNum)

	//b.ResetTimer()
	batch := db.NewBatch()
	fails := 0
	start := time.Now()

	for i := 0; i < entryNum; i++ {
		err := f(entries[i].key, entries[i].val, batch)
		if err != nil {
			b.Log("error rw ", "err", err.Error(), "dbType=", dbType, " testType=", testType) //, " key=", entries[i].key, " value=", entries[i].val)
			fails++
		}
	}
	batch.Write()

	b.Log("[took]", time.Since(start), "[fail]", fails)
}

func _Benchmark_Rsde(b *testing.B) {
	b.Log("start")
	b.Log(b.N)
	b.Log(b.N)
	b.Log(b.N)
	b.Log(b.N)
	b.Log(b.N)
	b.Log("end")
}

// go test -benchmem -run=^$ -bench ^(Benchmark_Rsde)$ -v

func createWorker(t *testing.T, quitCh, inChan chan int) {
	t.Log("start create worker")
	for {
		select {
		case i := <-inChan:
			t.Log("got input :", i)
			time.Sleep(time.Second)

		case <-quitCh:
			t.Log("end worker")
			return
		}
	}
}

func _TestChannels(t *testing.T) {
	quitChan := make(chan int)
	inChan := make(chan int, 10)
	go createWorker(t, quitChan, inChan)

	for i := 0; i < 20; i++ {
		inChan <- i
	}

	t.Log("closing")
	close(quitChan)

	time.Sleep(20 * time.Second)
}
