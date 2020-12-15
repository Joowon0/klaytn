package database

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const fetchNum = 100 // 20 batches : the number of items to fetch each time

var quitChan chan bool

var workspace = "/Users/joowon/dev/src/github.com/klaytn/klaytn/en_test/data/klay/chaindata/"

var ldbWorkSpace = workspace + "/" + dbBaseDirs[StateTrieDB]
var workspace2 = "/Users/joowon/dev/src/github.com/klaytn/klaytn/en_db_migration/data/klay/chaindata/"
var ldbWorkSpace2 = workspace2 + "/" + dbBaseDirs[StateTrieDB]

func TestMigrate(t *testing.T) {
	// create source DB
	dbc := &DBConfig{Dir: ldbWorkSpace, DBType: LevelDB, SingleDB: false, LevelDBCacheSize: 128, OpenFilesLimit: 128, NumStateTrieShards: 4}

	srcDB, err := newShardedDB(dbc, StateTrieDB, 4)
	if err != nil {
		t.Fatal("failed to create levelDB err:%w dbconfig:%w", err.Error(), dbc)
	}
	defer srcDB.Close()

	// create dst DB
	dbc2 := &DBConfig{Dir: ldbWorkSpace2, DBType: LevelDB, SingleDB: false, LevelDBCacheSize: 128, OpenFilesLimit: 128, NumStateTrieShards: 2}
	dstDB, err := newShardedDB(dbc2, StateTrieDB, 2)
	if err != nil {
		t.Fatal("failed to create levelDB err:%w dbconfig:%w", err.Error(), dbc)
	}
	defer dstDB.Close()

	// create src iterator and dst batch
	//it := NewshardedDBChIterator(nil, srcDB.shards)
	srcIter := srcDB.NewIterator()
	dstBatch := dstDB.NewBatch()

	// create iterator and iterate
	entries, fetched := iterateDB(t, srcIter, fetchNum)
	iterateNum := 0
	fmt.Println("first iterateDB", "fetched=", fetched)
	for len(entries) != 0 && fetched > 0 {
		fmt.Println("fetched items", " [count]", iterateNum, " [num]", fetched)
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
	time.Sleep(time.Second)
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
