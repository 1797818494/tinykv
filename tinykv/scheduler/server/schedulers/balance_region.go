// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	storeArray := make([]*core.StoreInfo, 0)
	// store还存活，且downTime < maxDownTime
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			storeArray = append(storeArray, store)
		}

	}
	// 只有两个的话就不调度
	if len(storeArray) < 2 {
		return nil
	}
	core.SortStoreArray(storeArray)

	var targetStore *core.StoreInfo = nil
	var sourceStore *core.StoreInfo = nil
	var region *core.RegionInfo
	// pending ---> follower --> leader(transfer)
	for _, store := range storeArray {
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(store.GetID(), func(rc core.RegionsContainer) {
			regions = rc
		})
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			sourceStore = store
			break
		}
		cluster.GetFollowersWithLock(store.GetID(), func(rc core.RegionsContainer) {
			regions = rc
		})
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			sourceStore = store
			break
		}
		cluster.GetLeadersWithLock(store.GetID(), func(rc core.RegionsContainer) {
			regions = rc
		})
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			sourceStore = store
			break
		}
	}
	if region == nil {
		return nil
	}
	if len(region.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}
	core.ReverseStoreArray(storeArray)
	for _, store := range storeArray {
		// 一个store只能有一个region 对象，所以要判断是否不存在
		if _, ok := region.GetStoreIds()[store.GetID()]; !ok {
			targetStore = store
			break
		}
	}
	if targetStore == nil {
		return nil
	}
	// < 2 倍没有必要迁移
	if sourceStore.GetRegionSize()-targetStore.GetRegionSize() < 2*region.GetApproximateSize() {
		return nil
	}
	// 分配新的peer(etcd allocId 获得持久化的id)
	newPeer, _ := cluster.AllocPeer(targetStore.GetID())
	op, _ := operator.CreateMovePeerOperator("", cluster, region, operator.OpBalance, sourceStore.GetID(), targetStore.GetID(), newPeer.Id)
	return op
}
