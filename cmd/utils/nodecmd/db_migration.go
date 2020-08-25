package nodecmd

import (
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/klaytn/klaytn/storage/database"

	"github.com/klaytn/klaytn/cmd/utils"
	"gopkg.in/urfave/cli.v1"
)

var (
	// key used to store entries in miscDB
	DBMigrationDSTInfoKey    = []byte("db_migration_dst")
	DBMigrationCheckpointKey = []byte("db_migration_checkpoint")

	dbFlags = []cli.Flag{
		// src DB
		utils.DbTypeFlag,
		utils.SingleDBFlag,
		utils.NumStateTrieShardsFlag,
		utils.DynamoDBTableNameFlag,
		utils.DynamoDBRegionFlag,
		utils.DynamoDBIsProvisionedFlag,
		utils.DynamoDBReadCapacityFlag,
		utils.DynamoDBWriteCapacityFlag,
		utils.LevelDBCompressionTypeFlag,
		utils.DataDirFlag,
	}
	dbMigrationFlags = append(dbFlags, DBMigrationFlags...)

	MigrationCommand = cli.Command{
		Name:     "db-migration",
		Usage:    "db migration",
		Flags:    []cli.Flag{},
		Category: "DB MIGRATION COMMANDS",
		Description: `

The migration commands ...
`,
		Subcommands: []cli.Command{
			{
				Name:   "status",
				Usage:  "Print status of db migration",
				Action: utils.MigrateFlags(statusMigration),
				Description: `
Print a short summary of db migration status`,
			},
			{
				Name:   "start",
				Usage:  "Start db migration",
				Flags:  dbMigrationFlags,
				Action: utils.MigrateFlags(startMigration),
				Description: `
    DB migration

Migrates a database to a different kind of DB.
(e.g. LevelDB -> BadgerDB, LevelDB -> DynamoDB)

If you want to move DB to a same kind, consider moving its place or using
built-in functions in DB. 

Do not use db migration while a node is executing.`,
			},
			{
				Name:   "pause",
				Usage:  "pause db migration",
				Action: utils.MigrateFlags(pauseMigration),
				Description: `
Pauses db migration.

Do not execute node before the db migration is finished.`,
			},
			{
				Name:   "stop",
				Usage:  "stop db migration",
				Action: utils.MigrateFlags(stopMigration),
				Description: `
Stops DB migration permanently.

This deletes the destination database.
Consider using pause if you want to continue db migration later.`,
			},
		},
	}
)

func statusMigration(ctx *cli.Context) error {
	srcDBConfig, _, err := createDBConfig(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to create db manager")
	}
	srcDBManager := database.NewDBManager(srcDBConfig)
	return srcDBManager.GetDBMigrationStatusInfo()
}

func startMigration(ctx *cli.Context) error {
	srcDBManager, dstDBManager, err := createDBManager(ctx)
	if err != nil {
		return err
	}
	return srcDBManager.StartDBMigration(dstDBManager)
}

func pauseMigration(ctx *cli.Context) error {
	srcDBConfig, _, err := createDBConfig(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to create db manager")
	}
	srcDBManager := database.NewDBManager(srcDBConfig)
	return srcDBManager.PauseDBMigration()
}

func stopMigration(ctx *cli.Context) error {
	srcDBConfig, _, err := createDBConfig(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to create db manager")
	}
	srcDBManager := database.NewDBManager(srcDBConfig)
	return srcDBManager.GetDBMigrationStatusInfo()
}

func createDBManager(ctx *cli.Context) (database.DBManager, database.DBManager, error) {
	// create db config from ctx
	srcDBConfig, dstDBConfig, dbManagerCreationErr := createDBConfig(ctx)
	srcDBManager := database.NewDBManager(srcDBConfig)

	// check and set dst DB config
	oldDstDBConfig, FetchDstDBErr := getDSTConfig(srcDBManager)
	if oldDstDBConfig != nil && FetchDstDBErr == nil { // use old dstDBManager if exist
		logger.Info("continuing db migration with previous settings")
		dstDBConfig = oldDstDBConfig
	} else if dbManagerCreationErr == nil { // store new dstDBManager
		logger.Info("starting a new db migration")
		if err := setDSTConfig(srcDBManager, *dstDBConfig); err != nil {
			return srcDBManager, nil, err
		}
	} else { // cannot fetch nor create dst DBManger
		return srcDBManager, nil, errors.New("Failed to get original DBConfig and create from context" +
			"[dbManagerCreationErr]" + dbManagerCreationErr.Error() +
			"[FetchDstDBErr]" + FetchDstDBErr.Error())
	}
	dstDBManager := database.NewDBManager(dstDBConfig)

	return srcDBManager, dstDBManager, nil
}

func createDBConfig(ctx *cli.Context) (*database.DBConfig, *database.DBConfig, error) {
	// srcDB
	srcDBC := &database.DBConfig{
		Dir:                ctx.GlobalString(utils.DataDirFlag.Name),
		DBType:             database.DBType(ctx.GlobalString(utils.DbTypeFlag.Name)).ToValid(),
		SingleDB:           ctx.GlobalBool(utils.SingleDBFlag.Name),
		NumStateTrieShards: ctx.GlobalUint(utils.NumStateTrieShardsFlag.Name),
		OpenFilesLimit:     database.GetOpenFilesLimit(),

		LevelDBCacheSize:   ctx.GlobalInt(utils.LevelDBCacheSizeFlag.Name),
		LevelDBCompression: database.LevelDBCompressionType(ctx.GlobalInt(utils.LevelDBCompressionTypeFlag.Name)),

		DynamoDBConfig: &database.DynamoDBConfig{
			TableName:          ctx.GlobalString(utils.DynamoDBTableNameFlag.Name),
			Region:             ctx.GlobalString(utils.DynamoDBRegionFlag.Name),
			IsProvisioned:      ctx.GlobalBool(utils.DynamoDBIsProvisionedFlag.Name),
			ReadCapacityUnits:  ctx.GlobalInt64(utils.DynamoDBReadCapacityFlag.Name),
			WriteCapacityUnits: ctx.GlobalInt64(utils.DynamoDBWriteCapacityFlag.Name),
		},
	}
	if len(srcDBC.DBType) == 0 { // changed to invalid type
		return nil, nil, errors.New("srcDB is not specified or invalid : " + ctx.GlobalString(utils.DbTypeFlag.Name))
	}

	// dstDB
	dstDBC := &database.DBConfig{
		Dir:                ctx.GlobalString(utils.DstDataDirFlag.Name),
		DBType:             database.DBType(ctx.GlobalString(utils.DstDbTypeFlag.Name)).ToValid(),
		SingleDB:           ctx.GlobalBool(utils.DstSingleDBFlag.Name),
		NumStateTrieShards: ctx.GlobalUint(utils.DstNumStateTrieShardsFlag.Name),
		OpenFilesLimit:     database.GetOpenFilesLimit(),

		LevelDBCacheSize:   ctx.GlobalInt(utils.DstLevelDBCacheSizeFlag.Name),
		LevelDBCompression: database.LevelDBCompressionType(ctx.GlobalInt(utils.DstLevelDBCompressionTypeFlag.Name)),

		DynamoDBConfig: &database.DynamoDBConfig{
			TableName:          ctx.GlobalString(utils.DstDynamoDBTableNameFlag.Name),
			Region:             ctx.GlobalString(utils.DstDynamoDBRegionFlag.Name),
			IsProvisioned:      ctx.GlobalBool(utils.DstDynamoDBIsProvisionedFlag.Name),
			ReadCapacityUnits:  ctx.GlobalInt64(utils.DstDynamoDBReadCapacityFlag.Name),
			WriteCapacityUnits: ctx.GlobalInt64(utils.DstDynamoDBWriteCapacityFlag.Name),
		},
	}
	if len(dstDBC.DBType) == 0 { // changed to invalid type
		return nil, nil, errors.New("dstDB is not specified or invalid : " + ctx.GlobalString(utils.DstDbTypeFlag.Name))
	}

	return srcDBC, dstDBC, nil
}

// TODO put db related functions in database package
func setDSTConfig(srcDB database.DBManager, dstDBConfig database.DBConfig) error {
	miscDB := srcDB.GetMiscDB()

	marshaled, err := json.Marshal(dstDBConfig)
	if err != nil {
		return errors.Wrap(err, "failed to marshal DBConfig")
	}

	return miscDB.Put(DBMigrationDSTInfoKey, marshaled)
}

func getDSTConfig(srcDB database.DBManager) (*database.DBConfig, error) {
	miscDB := srcDB.GetMiscDB()

	fetchedConfig, err := miscDB.Get(DBMigrationDSTInfoKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch db info from db")
	}

	var dstConfig *database.DBConfig
	err = json.Unmarshal(fetchedConfig, dstConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal dbconfig")
	}

	return dstConfig, nil
}
