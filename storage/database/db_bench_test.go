package database

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	_ "strconv"
	"syscall"
	"testing"
	"time"

	"github.com/klaytn/klaytn/log"
	"github.com/klaytn/klaytn/log/term"
	"github.com/mattn/go-colorable"
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

const workspace = "/home/ubuntu/klaytn/benchmark_data"
const ldbWorkSpace = workspace + "/ldb"

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

	keys := MakeRandomBytesSlice(256, entryNum)
	values := MakeRandomBytesSlice(600, entryNum)
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

const entryNum = 1000

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
	enableLog()
	benchDB(b, DynamoDB, "get")
}
func Test_DynamoWrite(b *testing.T) {
	enableLog()
	benchDB(b, DynamoDB, "put")
}

func Test_DynamoBatchWrite(b *testing.T) {
	enableLog()
	benchDB(b, DynamoDB, "batchWrite")
}

func benchDB(b *testing.T, dbType DBType, testType string) {
	// ldbWorkSpace + strconv.Itoa(entryNum)
	dbc := &DBConfig{Dir: workspace + "/to", DBType: dbType, SingleDB: true, LevelDBCacheSize: 128, OpenFilesLimit: 128,
		NumStateTrieShards: 1, DynamoDBConfig: GetDefaultDynamoDBConfig()}
	dbc.DynamoDBConfig.TableName = "winnie-migration"
	dbcjson, _ := json.Marshal(*dbc)
	b.Log(string(dbcjson))
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
			b.FailNow()
			fails++
		}
	}
	batch.Write()

	b.Log("[took]", time.Since(start), "[fail]", fails)
}

func MakeRandomBytesSlice(length int, num int) [][]byte {
	rand.Seed(time.Now().UTC().UnixNano())
	result := make([][]byte, num)
	for i := 0; i < num; i++ {
		result[i] = make([]byte, length)
		rand.Read(result[i])
	}
	return result
}

func TestIterator(t *testing.T) {
	enableLog()
	// "/copied"
	dbc := &DBConfig{Dir: workspace + "/noShard", DBType: LevelDB, SingleDB: false, LevelDBCacheSize: 128, OpenFilesLimit: 128,
		NumStateTrieShards: 1, DynamoDBConfig: GetDefaultDynamoDBConfig()}
	dbc.DynamoDBConfig.TableName = "winnie-migration"
	dbm := NewDBManager(dbc)
	db := dbm.GetStateTrieDB()
	defer dbm.Close()
	t.Log(dbc.Dir)

	it := db.NewIteratorWithStart([]byte{})
	if it == nil {
		t.FailNow()
	}
	entries, fetched := iterateDB(it, fetchNum)
	// iterateNum := 0
	logger.Info("itertated", "len(entries)", len(entries), "fetched", fetched) //, "key", entries[0])
	entries, fetched = iterateDB(it, fetchNum)
	logger.Info("itertated", "len(entries)", len(entries), "fetched", fetched) //, "key", entries[0]), "key", entries[0])
	entries, fetched = iterateDB(it, fetchNum)
	logger.Info("itertated", "len(entries)", len(entries), "fetched", fetched) //, "key", entries[0]), "key", entries[0])

	it.Release()
	err := it.Error()
	if err != nil {
		assert.NoError(t, err)
	}
}

func TestSignal(t *testing.T) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

loop:
	for i := 0; i < 20; i++ {
		fmt.Println(i)
		select {
		case <-sigc:
			fmt.Println("exit called", i)
			break loop
		default:
		}
		time.Sleep(1 * time.Second)
	}
}
