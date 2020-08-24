package nodecmd

import (
	"github.com/pkg/errors"

	"github.com/klaytn/klaytn/storage/database"

	"github.com/klaytn/klaytn/cmd/utils"
	"gopkg.in/urfave/cli.v1"
)

var (
	DstDbTypeFlag = cli.StringFlag{
		Name:  "dst.dbtype",
		Usage: `Blockchain storage database type ("LevelDB", "BadgerDB", "DynamoDBS3")`,
		Value: "LevelDB",
	}

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
				Name:      "start",
				Usage:     "Start db migration",
				ArgsUsage: "",
				Flags: []cli.Flag{
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

					// dst DB
					DstDbTypeFlag,
				},
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
	srcDBManager, _, err := getDBManager(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to create db manager")
	}
	return srcDBManager.GetDBMigrationStatusInfo()
}

func startMigration(ctx *cli.Context) error {
	srcDBManager, dstDBManager, err := getDBManager(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to create db manager")
	}
	return srcDBManager.StartDBMigration(dstDBManager)
}

func pauseMigration(ctx *cli.Context) error {
	srcDBManager, _, err := getDBManager(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to create db manager")
	}
	return srcDBManager.PauseDBMigration()
}

func stopMigration(ctx *cli.Context) error {
	srcDBManager, _, err := getDBManager(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to create db manager")
	}
	return srcDBManager.GetDBMigrationStatusInfo()
}

func getDBManager(ctx *cli.Context) (database.DBManager, database.DBManager, error) {
	// global vars
	srcDBType := database.LevelDB
	if dbtype := database.DBType(ctx.GlobalString(utils.DbTypeFlag.Name)).ToValid(); len(dbtype) != 0 {
		srcDBType = dbtype
	}
	var dstDBType database.DBType
	if dstDBType = database.DBType(ctx.GlobalString(DstDbTypeFlag.Name)).ToValid(); len(dstDBType) != 0 {
		// TODO-Klaytn dstDB is necessary for status, pause, stop
		return nil, nil, errors.New("dstDB is not specified")
	}
	singleDB := ctx.GlobalBool(utils.SingleDBFlag.Name)
	numStateTrieShards := ctx.GlobalUint(utils.NumStateTrieShardsFlag.Name)

	// open src DB
	srcDBC := &database.DBConfig{Dir: "chaindata", DBType: srcDBType,
		SingleDB: singleDB, NumStateTrieShards: numStateTrieShards,
		LevelDBCacheSize: 0, OpenFilesLimit: 0}
	srcDBManager := database.NewDBManager(srcDBC)

	// open dst DB
	dstDBC := &database.DBConfig{Dir: "chaindata", DBType: dstDBType,
		SingleDB: singleDB, NumStateTrieShards: numStateTrieShards,
		LevelDBCacheSize: 0, OpenFilesLimit: 0}
	dstDBManager := database.NewDBManager(dstDBC)

	return srcDBManager, dstDBManager, nil
}
