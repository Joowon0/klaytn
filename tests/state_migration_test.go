// Copyright 2020 The klaytn Authors
// This file is part of the klaytn library.
//
// The klaytn library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The klaytn library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the klaytn library. If not, see <http://www.gnu.org/licenses/>.

package tests

import (
	"encoding/json"
	"github.com/klaytn/klaytn/common"
	"github.com/klaytn/klaytn/node"
	"github.com/klaytn/klaytn/node/cn"
	"github.com/klaytn/klaytn/params"
	"github.com/klaytn/klaytn/reward"
	"github.com/klaytn/klaytn/storage/database"
	"github.com/stretchr/testify/assert"
	"math/big"
	"os"
	"strconv"
	"testing"
	"time"
)

// continues occurrence of state trie migration and node restart must success
func TestMigration_ContinuesRestartAndMigration(t *testing.T) {
	fullNode, node, validator, chainID, workspace, richAccount, _, _ := newSimpleBlockchain(t, params.RoundRobin, 10)
	defer os.RemoveAll(workspace)

	stateTriePath := []byte("statetrie")

	numTxs := []int{10, 100}
	for i := 0; i < len(numTxs); i++ {
		numTx := numTxs[i%len(numTxs)]
		t.Log("attempt", strconv.Itoa(i), " : deployRandomTxs of", strconv.Itoa(numTx))
		deployRandomTxs(t, node.TxPool(), chainID, richAccount, numTx)
		time.Sleep(3 * time.Second) // wait until txpool is flushed

		startMigration(t, node)
		time.Sleep(1 * time.Second)

		t.Log("migration state before restart", node.ChainDB().InMigration())
		fullNode, node = restartNode(t, fullNode, node, workspace, validator)

		waitMigrationEnds(t, node)

		// check if migration succeeds (StateTrieDB changes when migration finishes)
		newPathKey := append([]byte("databaseDirectory"), common.Int64ToByteBigEndian(uint64(database.StateTrieDB))...)
		newStateTriePath, err := node.ChainDB().GetMiscDB().Get(newPathKey)
		assert.NoError(t, err)
		assert.NotEqual(t, stateTriePath, newStateTriePath)
		stateTriePath = newStateTriePath
	}

	stopNode(t, fullNode)
}

// if migration status is set on miscDB and a node is restarted, migration should start
func TestMigration_StartMigrationByMiscDB(t *testing.T) {
	fullNode, node, validator, chainID, workspace, richAccount, _, _ := newSimpleBlockchain(t, params.RoundRobin, 10)
	defer os.RemoveAll(workspace)
	miscDB := node.ChainDB().GetMiscDB()

	// size up state trie to be prepared for migration
	deployRandomTxs(t, node.TxPool(), chainID, richAccount, 100)
	time.Sleep(time.Second)

	// set migration status in miscDB
	migrationBlockNum := node.BlockChain().CurrentBlock().Header().Number.Uint64()
	err := miscDB.Put([]byte("migrationStatus"), common.Int64ToByteBigEndian(migrationBlockNum))
	assert.NoError(t, err)

	// set migration db path in miscDB
	migrationPathKey := append([]byte("databaseDirectory"), common.Int64ToByteBigEndian(uint64(database.StateTrieMigrationDB))...)
	migrationPath := []byte("statetrie_migrated_" + strconv.FormatUint(migrationBlockNum, 10))
	err = miscDB.Put(migrationPathKey, migrationPath)
	assert.NoError(t, err)

	// check migration Status in cache before restart
	assert.False(t, node.ChainDB().InMigration())
	assert.NotEqual(t, migrationBlockNum, node.ChainDB().MigrationBlockNumber())

	fullNode, node = restartNode(t, fullNode, node, workspace, validator)
	miscDB = node.ChainDB().GetMiscDB()

	// check migration Status in cache after restart
	if node.ChainDB().InMigration() {
		assert.Equal(t, migrationBlockNum, node.ChainDB().MigrationBlockNumber())
		t.Log("Checked migration status while migration in on process")
	}

	waitMigrationEnds(t, node)

	// state trie path should not be "statetrie" in miscDB
	newPathKey := append([]byte("databaseDirectory"), common.Int64ToByteBigEndian(uint64(database.StateTrieDB))...)
	dir, err := miscDB.Get(newPathKey)
	assert.NoError(t, err)
	assert.NotEqual(t, "statetrie", string(dir))

	stopNode(t, fullNode)
}

// staking information should be stored in miscDB
func TestMigration_StakinInfoDBInMiscDB(t *testing.T) {
	params.SetStakingUpdateInterval(3)
	_, node, _, chainID, workspace, richAccount, _, _ := newSimpleBlockchain(t, params.WeightedRandom, 10)
	defer os.RemoveAll(workspace)
	miscDB := node.ChainDB().GetMiscDB()

	// wait for staking info to be made
	time.Sleep(time.Duration(2*params.StakeUpdateInterval) * time.Second)

	// check if staking info is stored in DB before migration
	checkStakingInfoInDB(t, miscDB, node.BlockChain().CurrentHeader().Number.Uint64())

	// size up state trie to be prepared for migration
	deployRandomTxs(t, node.TxPool(), chainID, richAccount, 100)
	time.Sleep(time.Second)

	// check if staking info is stored in DB on prerequisite of migration
	startMigration(t, node)
	time.Sleep(1 * time.Second)
	checkStakingInfoInDB(t, miscDB, node.BlockChain().CurrentHeader().Number.Uint64())
	checkStakingInfoInDB(t, miscDB, node.BlockChain().CurrentHeader().Number.Uint64()+params.StakingUpdateInterval())

	// check if staking info is sotred in DB after migration
	waitMigrationEnds(t, node)
	time.Sleep(time.Duration(params.StakeUpdateInterval) * time.Second)
	checkStakingInfoInDB(t, miscDB, node.BlockChain().CurrentHeader().Number.Uint64())
}

// migration should not start if staking information is not stored
func TestMigration_PrerequisiteStakingInfoFail(t *testing.T) {
	fullNode, node, validator, chainID, workspace, richAccount, _, _ := newSimpleBlockchain(t, params.WeightedRandom, 10)
	defer os.RemoveAll(workspace)
	miscDB := node.ChainDB().GetMiscDB()

	// size up state trie to be prepared for migration
	deployRandomTxs(t, node.TxPool(), chainID, richAccount, 100)
	time.Sleep(time.Second)

}

// migration should not start if staking information is not stored
func TestMigration_PrerequisiteMiscDBFail(t *testing.T) {
	fullNode, node, validator, chainID, workspace, richAccount, _, _ := newSimpleBlockchain(t, params.WeightedRandom, 10)
	defer os.RemoveAll(workspace)
	miscDB := node.ChainDB().GetMiscDB()

	// size up state trie to be prepared for migration
	deployRandomTxs(t, node.TxPool(), chainID, richAccount, 100)
	time.Sleep(time.Second)

}

func newSimpleBlockchain(t *testing.T, proposerPolicy uint64, numAccounts int) (*node.Node, *cn.CN, *TestAccountType, *big.Int, string, *TestAccountType, []*TestAccountType, []*TestAccountType) {
	//if testing.Verbose() {
	//	enableLog() // Change verbosity level in the function if needed
	//}

	t.Log("=========== create blockchain ==============")
	fullNode, node, validator, chainID, workspace := newBlockchain(t, proposerPolicy)
	richAccount, accounts, contractAccounts := createAccount(t, numAccounts, validator)
	time.Sleep(5 * time.Second)

	return fullNode, node, validator, chainID, workspace, richAccount, accounts, contractAccounts
}

func startMigration(t *testing.T, node *cn.CN) {
	waitMigrationEnds(t, node)

	t.Log("=========== migrate trie ==============")
	err := node.BlockChain().PrepareStateMigration()
	assert.NoError(t, err)
}

func restartNode(t *testing.T, fullNode *node.Node, node *cn.CN, workspace string, validator *TestAccountType) (*node.Node, *cn.CN) {
	stopNode(t, fullNode)
	time.Sleep(2 * time.Second)
	newFullNode, newNode := startNode(t, workspace, validator)
	time.Sleep(2 * time.Second)

	return newFullNode, newNode
}

func startNode(t *testing.T, workspace string, validator *TestAccountType) (fullNode *node.Node, node *cn.CN) {
	t.Log("=========== starting node ==============")
	newFullNode, newNode := newKlaytnNode(t, workspace, validator, params.RoundRobin)
	if err := newNode.StartMining(false); err != nil {
		t.Fatal()
	}

	return newFullNode, newNode
}

func stopNode(t *testing.T, fullNode *node.Node) {
	if err := fullNode.Stop(); err != nil {
		t.Fatal(err)
	}
	t.Log("=========== stopped node ==============")
}

func waitMigrationEnds(t *testing.T, node *cn.CN) {
	for node.ChainDB().InMigration() {
		t.Log("state trie migration is processing; sleep for a second before a new migration")
		time.Sleep(time.Second)
	}
}

func checkStakingInfoInDB(t *testing.T, miscDB database.Database, currentNum uint64) {
	stakingNum := params.CalcStakingBlockNumber(currentNum)
	jsonByte, err := miscDB.Get(append([]byte("stakingInfo"), common.Int64ToByteLittleEndian(stakingNum)...))
	assert.NoError(t, err)
	stakingInfo := new(reward.StakingInfo)
	assert.NoError(t, json.Unmarshal(jsonByte, stakingInfo))
	assert.Equal(t, reward.GetStakingInfo(currentNum), stakingInfo)
}
