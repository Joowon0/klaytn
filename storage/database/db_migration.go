package database

import (
	"encoding/json"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
)

var (
	DBMigrationCheckpointKey = []byte("db_migration_checkpoint")
)

const fetchNum = 500 // 20 batches : the number of items to fetch each time
type entry struct {
	key []byte
	val []byte
}

func (dbm *databaseManager) GetDBMigrationStatusInfo() error {
	return errors.New("Get DB migration status is not implemented")
}

func (dbm *databaseManager) StartDBMigration(dstdbm DBManager) error {
	// s, _ := json.Marshal(dbm.config)
	// fmt.Println(string(s))
	// d, _ := json.Marshal(dstdbm.GetDBConfig())
	// fmt.Println(string(d))
	//TODO setup signal interrupt
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	// get checkpoint if exits
	// checkpoint, err := dbm.getDBMigrationCheckpoint()

	// TODO enable for all dbs
	srcDB := dbm.dbs[0]
	dstDB := dstdbm.getDatabase(DBEntryType(MiscDB))

	// create src iterator and dst batch
	srcIter := srcDB.NewIterator()
	dstBatch := dstDB.NewBatch()

	// create iterator and iterate
	entries, fetched := iterateDB(srcIter, fetchNum)
	iterateNum := 0
loop:
	for len(entries) != 0 && fetched > 0 {
		logger.Info("fetched items", " [count]", iterateNum, " [num]", fetched)
		for i := 0; i < fetched; i++ {
			dstBatch.Put(entries[i].key, entries[i].val)
		}

		select {
		case <-sigc:
			logger.Info("exit called")
			// dbm.setDBMigrationCheckpoint(checkpoint)
			break loop
		default:
		}

		entries, fetched = iterateDB(srcIter, fetchNum)
		iterateNum++
	}

	err := dstBatch.Write()
	if err != nil {
		return errors.Wrap(err, "failed to write items")
	}

	srcIter.Release()
	err = srcIter.Error()
	if err != nil {
		return errors.Wrap(err, "failed to iterate")
	}

	return nil
}

func iterateDB(iter Iterator, num int) ([]entry, int) {
	entries := make([]entry, num)
	var i int
	for i = 0; i < num && iter.Next(); i++ {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		val := iter.Value()

		// logger.Info("fetched", "key", key, "val", val)

		entries[i].key = make([]byte, len(key))
		entries[i].val = make([]byte, len(val))
		copy(entries[i].key, key)
		copy(entries[i].val, val)
	}

	return entries, i
}

func (dbm *databaseManager) PauseDBMigration() error {
	return errors.New("Pause DB Migration is not implemented")
}

func (dbm *databaseManager) StopDBMigration() error {
	return nil
}

// db migration checkpoint
type DBMigrationCheckpoint struct {
	db         DBEntryType // db index
	checkpoint []byte
}

func (dbm *databaseManager) setDBMigrationCheckpoint(until []byte) error {
	return nil

	// return miscDB.Put(DBMigrationCheckpointKey, marshaled)
}

func (dbm *databaseManager) getDBMigrationCheckpoint() (*DBMigrationCheckpoint, error) {
	miscDB := dbm.GetMiscDB()

	fetchedConfig, err := miscDB.Get(DBMigrationCheckpointKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch db info from db")
	}

	var dstConfig *DBMigrationCheckpoint
	err = json.Unmarshal(fetchedConfig, dstConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal dbconfig")
	}

	return dstConfig, nil
}
