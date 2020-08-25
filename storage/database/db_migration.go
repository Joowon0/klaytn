package database

import (
	"encoding/json"
	"fmt"

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
	fmt.Println(dbm.config)
	fmt.Println(dstdbm.GetDBConfig())
	//TODO setup signal interrupt
	//sigc := make(chan os.Signal, 1)
	//signal.Notify(sigc,
	//	syscall.SIGHUP,
	//	syscall.SIGINT,
	//	syscall.SIGTERM,
	//	syscall.SIGQUIT)

	//// get checkpoint if exits
	//checkpoint, err := dbm.getDBMigrationCheckpoint()
	//dbStartpoint := MiscDB
	//if checkpoint != nil || err == nil {
	//	dbStartpoint = checkpoint.db
	//}
	//
	//// for each DB
	//for i, srcDB := range dbm.dbs[dbStartpoint:] {
	//	dstDB := dstdbm.getDatabase(DBEntryType(i))
	//
	//	// create src iterator and dst batch
	//	srcIter := srcDB.NewIterator()
	//	dstBatch := dstDB.NewBatch()
	//
	//	// create iterator and iterate
	//	entries, fetched := iterateDB(srcIter, fetchNum)
	//	iterateNum := 0
	//	for len(entries) != 0 && fetched > 0 {
	//		logger.Info("fetched items", " [count]", iterateNum, " [num]", fetched)
	//		for i := 0; i < fetched; i++ {
	//			dstBatch.Put(entries[i].key, entries[i].val)
	//		}
	//
	//		entries, fetched = iterateDB(srcIter, 500)
	//		iterateNum++
	//	}
	//
	//	err := dstBatch.Write()
	//	if err != nil {
	//		errors.Wrap(err, "failed to write items in DynamoDB")
	//	}
	//
	//	srcIter.Release()
	//	err = srcIter.Error()
	//	if err != nil {
	//		errors.Wrap(err, "failed to iterate levelDB")
	//	}
	//
	//	return nil
	//}

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

// db migrationcheckpoint
type DBMigrationCheckpoint struct {
	db         DBEntryType // db index
	checkpoint []byte
}

func (dbm *databaseManager) setDBMigrationCheckpoint(checkpoint DBMigrationCheckpoint) error {
	miscDB := dbm.GetMiscDB()

	marshaled, err := json.Marshal(checkpoint)
	if err != nil {
		return errors.Wrap(err, "failed to marshal DBConfig")
	}

	return miscDB.Put(DBMigrationCheckpointKey, marshaled)
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
