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
	"github.com/jinzhu/copier"
	"sync"
)

const (
	maxStakingCache = 4
)

type stakingInfoCache struct {
	cells       map[uint64]*StakingInfo
	minBlockNum uint64
	lock        sync.RWMutex
}

func newStakingInfoCache() *stakingInfoCache {
	stakingCache := new(stakingInfoCache)
	stakingCache.cells = make(map[uint64]*StakingInfo)
	return stakingCache
}

func (sc *stakingInfoCache) get(blockNum uint64) *StakingInfo {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	s, ok := sc.cells[blockNum]
	if !ok {
		return nil
	}

	stakingInfo := newEmptyStakingInfo(blockNum)
	err := copier.Copy(stakingInfo, s)
	if err != nil {
		return nil
	}
	return stakingInfo

}

func (sc *stakingInfoCache) add(stakingInfo *StakingInfo) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	// Assumption: stakingInfo is not nil.

	if len(sc.cells) >= maxStakingCache {
		delete(sc.cells, sc.minBlockNum)
	}
	sc.minBlockNum = stakingInfo.BlockNum
	for _, s := range sc.cells {
		if s.BlockNum < sc.minBlockNum {
			sc.minBlockNum = s.BlockNum
		}
	}
	sc.cells[stakingInfo.BlockNum] = stakingInfo
	logger.Debug("Add a new stakingInfo to stakingInfoCache", "blockNum", stakingInfo.BlockNum)
}
