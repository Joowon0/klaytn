// Copyright 2019 The klaytn Authors
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

package reward

import (
	"errors"
	"github.com/klaytn/klaytn/blockchain"
	"github.com/klaytn/klaytn/blockchain/state"
	"github.com/klaytn/klaytn/blockchain/types"
	"github.com/klaytn/klaytn/common"
	"github.com/klaytn/klaytn/event"
	"github.com/klaytn/klaytn/params"
)

const (
	chainHeadChanSize = 100
)

// blockChain is an interface for blockchain.Blockchain used in reward package.
type blockChain interface {
	SubscribeChainHeadEvent(ch chan<- blockchain.ChainHeadEvent) event.Subscription
	GetBlockByNumber(number uint64) *types.Block
	StateAt(root common.Hash) (*state.StateDB, error)
	Config() *params.ChainConfig

	blockchain.ChainContext
}

type StakingManager struct {
	addressBookConnector *addressBookConnector
	stakingInfoCache     *stakingInfoCache
	stakingInfoDB        stakingInfoDB
	governanceHelper     governanceHelper
	blockchain           blockChain
	chainHeadChan        chan blockchain.ChainHeadEvent
	chainHeadSub         event.Subscription
	isActivated          bool
}

func NewStakingManager(bc blockChain, gh governanceHelper, db stakingInfoDB) *StakingManager {
	return &StakingManager{
		addressBookConnector: newAddressBookConnector(bc, gh),
		stakingInfoCache:     newStakingInfoCache(),
		stakingInfoDB:        db,
		governanceHelper:     gh,
		blockchain:           bc,
		chainHeadChan:        make(chan blockchain.ChainHeadEvent, chainHeadChanSize),
		isActivated:          false,
	}
}

func (sm *StakingManager) IsActivated() bool {
	return sm.isActivated
}

// GetStakingInfo gets staking info of given blockNum from either cache, db or address book.
func (sm *StakingManager) GetStakingInfo(blockNum uint64) *StakingInfo {
	stakingBlockNumber := params.CalcStakingBlockNumber(blockNum)

	// Get staking info from cache
	if cachedStakingInfo := sm.stakingInfoCache.get(stakingBlockNumber); cachedStakingInfo != nil {
		logger.Debug("StakingInfoCache hit.", "blockNum", blockNum, "staking block number", stakingBlockNumber, "stakingInfo", cachedStakingInfo)
		return cachedStakingInfo
	}

	// Get staking info from DB
	if storedStakingInfo, err := sm.getStakingInfoFromDB(stakingBlockNumber); storedStakingInfo != nil && err == nil {
		logger.Debug("StakingInfoDB hit.", "blockNum", blockNum, "staking block number", stakingBlockNumber, "stakingInfo", storedStakingInfo)
		sm.stakingInfoCache.add(storedStakingInfo)
		return storedStakingInfo
	}

	// Get staking info from address book
	if calcStakingInfo, err := sm.addressBookConnector.getStakingInfoFromAddressBook(stakingBlockNumber); calcStakingInfo != nil && err == nil {
		logger.Debug("Fetched stakingInfo from address book.", "blockNum", blockNum, "staking block number", stakingBlockNumber, "stakingInfo", calcStakingInfo)
		sm.stakingInfoCache.add(calcStakingInfo)
		return calcStakingInfo
	}

	logger.Error("Failed to get stakingInfo from cache, DB and address book", "blockNum", blockNum, "staking block number", stakingBlockNumber)
	return nil
}

// SetStakingInfoCache stores staking info in cache.
// Staking info cache stores at most maxStakingCache.
func (sm *StakingManager) SetStakingInfoCache(stakingInfo *StakingInfo) {
	sm.isActivated = true
	sm.stakingInfoCache.add(stakingInfo)
}

// SetStakingInfoDB gets staking info from cache and stores it in db.
func (sm *StakingManager) SetStakingInfoDB(blockNum uint64) error {
	stakingBlockNumber := params.CalcStakingBlockNumber(blockNum)

	cachedStakingInfo := sm.stakingInfoCache.get(stakingBlockNumber)
	if cachedStakingInfo == nil {
		return errors.New("unable to get staking info from cache")
	}

	if err := sm.addStakingInfoToDB(cachedStakingInfo); err != nil {
		logger.Error("Failed to write staking info to db.", "err", err, "stakingInfo", cachedStakingInfo)
		return err
	}

	return nil
}

// UpdateStakingInfo fetch staking info from address book and stores it in cache.
func (sm *StakingManager) UpdateStakingInfo(blockNum uint64) (*StakingInfo, error) {
	stakingBlockNumber := params.CalcStakingBlockNumber(blockNum)

	calcStakingInfo, err := sm.addressBookConnector.getStakingInfoFromAddressBook(stakingBlockNumber)
	if calcStakingInfo == nil || err != nil {
		return calcStakingInfo, err
	}

	sm.stakingInfoCache.add(calcStakingInfo)
	return calcStakingInfo, nil
}

// Subscribe setups a channel to listen chain head event and starts a goroutine to update staking cache.
func (sm *StakingManager) Subscribe() {
	sm.chainHeadSub = sm.blockchain.SubscribeChainHeadEvent(sm.chainHeadChan)

	go sm.handleChainHeadEvent()
}

func (sm *StakingManager) handleChainHeadEvent() {
	defer sm.Unsubscribe()

	logger.Info("Start listening chain head event to update stakingInfoCache.")

	for {
		// A real event arrived, process interesting content
		select {
		// Handle ChainHeadEvent
		case ev := <-sm.chainHeadChan:
			if sm.governanceHelper.ProposerPolicy() == params.WeightedRandom {
				stakingInfo, err := sm.UpdateStakingInfo(ev.Block.NumberU64())
				if stakingInfo == nil || err != nil {
					logger.Error("Failed to update stakingInfoCache", "blockNumber", ev.Block.NumberU64(), "err", err)
				}
			}
		case <-sm.chainHeadSub.Err():
			return
		}
	}
}

// Unsubscribe can unsubscribe a subscription to listen chain head event.
func (sm *StakingManager) Unsubscribe() {
	sm.chainHeadSub.Unsubscribe()
}
