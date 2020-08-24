package database

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

const fetchNum = 500 // 20 batches : the number of items to fetch each time
var quitChan chan bool

var workspace = "/home/ubuntu/klaytn/benchmark_data"
var ldbWorkSpace = workspace + "/ldb"

type entry struct {
	key []byte
	val []byte
}

func TestMigrate(t *testing.T) {
	// create source DB
	dbc := &DBConfig{Dir: ldbWorkSpace + strconv.Itoa(entryNum), DBType: LevelDB, SingleDB: true, LevelDBCacheSize: 128, OpenFilesLimit: 128}
	srcDB, err := NewLevelDB(dbc, 0)
	if err != nil {
		t.Fatal("failed to create levelDB err:%w dbconfig:%w", err.Error(), dbc)
	}
	defer srcDB.Close()

	// create dst DB
	ddbc := GetDefaultDynamoDBConfig() // dynamoDB configuration
	ddbc.TableName = "winnie-migration"
	dstDB, err := NewDynamoDB(ddbc)
	if err != nil {
		t.Fatal("failed to create dynamoDB err:%w dbconfig:%w", err.Error(), ddbc)
	}
	defer dstDB.Close()

	// create src iterator and dst batch
	srcIter := srcDB.NewIterator()
	dstBatch := dstDB.NewBatch()

	// create iterator and iterate
	entries, fetched := iterateDB(t, srcIter, fetchNum)
	iterateNum := 0
	for len(entries) != 0 && fetched > 0 {
		t.Log("fetched items", " [count]", iterateNum, " [num]", fetched)
		for i := 0; i < fetched; i++ {
			dstBatch.Put(entries[i].key, entries[i].val)
		}

		entries, fetched = iterateDB(t, srcIter, 500)
		iterateNum++
	}

	err = dstBatch.Write()
	assert.NoError(t, err, "failed to write items in DynamoDB")

	srcIter.Release()
	err = srcIter.Error()
	assert.NoError(t, err, "failed to iterate levelDB")
}

func TestStop(t *testing.T) {
	close(quitChan)
}

func iterateDB(t *testing.T, iter Iterator, num int) ([]entry, int) {
	entries := make([]entry, num)
	var i int
	for i = 0; i < num && iter.Next(); i++ {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		val := iter.Value()

		entries[i].key = make([]byte, len(key))
		entries[i].val = make([]byte, len(val))
		copy(entries[i].key, key)
		copy(entries[i].val, val)
	}

	return entries, i
}
