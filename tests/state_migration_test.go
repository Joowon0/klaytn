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
	"github.com/klaytn/klaytn/node"
	"github.com/klaytn/klaytn/node/cn"
	"math/big"
	"testing"
	"time"
)

func TestMigration_ContinuesRestartAndMigration(t *testing.T) {
	numAccounts := 100
	_, node, _, chainId, _, richAccount, _, _ := newSimpleBlockchain(t, numAccounts)
	time.Sleep(20 * time.Second)

	db, _ := node.BlockChain().TryGetRecentCachedStateDB()
	t.Log("richAccount Balance", db.GetBalance(richAccount.Addr))

	for i := 0; i < 5; i++ {
		deployRandomTxs(t, node.TxPool(), chainId, richAccount, 10)
		//deployValueTransferTx(t, node.TxPool(), chainId, richAccount, accounts[i%numAccounts])
		//deployContractExecutionTx(t, node.TxPool(), chainId, richAccount, contractAccounts[i%numAccounts].Addr)
		time.Sleep(5 * time.Second) // wait until txpool is flushed

		db, _ := node.BlockChain().TryGetRecentCachedStateDB()
		t.Log("richAccount Balance", db.GetBalance(richAccount.Addr))

		//startMigration(t, node)
		//restartNode(t, fullNode, node, workspace, validator)
	}
}

func newSimpleBlockchain(t *testing.T, numAccounts int) (*node.Node, *cn.CN, *TestAccountType, *big.Int, string, *TestAccountType, []*TestAccountType, []*TestAccountType) {
	if testing.Verbose() {
		enableLog() // Change verbosity level in the function if needed
	}

	t.Log("=========== create blockchain ==============")
	fullNode, node, validator, chainId, workspace := newBlockchain(t)
	richAccount, accounts, contractAccounts := createAccount(t, numAccounts, validator)
	time.Sleep(10 * time.Second)

	return fullNode, node, validator, chainId, workspace, richAccount, accounts, contractAccounts
}

func startMigration(t *testing.T, node *cn.CN) {
	t.Log("=========== migrate trie ==============")
	if err := node.BlockChain().PrepareStateMigration(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Second)
}

func restartNode(t *testing.T, fullNode *node.Node, node *cn.CN, workspace string, validator *TestAccountType) {
	t.Log("=========== stop node ==============")
	if err := fullNode.Stop(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Second)
	t.Log("=========== start node ==============")
	fullNode, node = newKlaytnNode(t, workspace, validator)
	if err := node.StartMining(false); err != nil {
		t.Fatal()
	}
	time.Sleep(10 * time.Second)
}
