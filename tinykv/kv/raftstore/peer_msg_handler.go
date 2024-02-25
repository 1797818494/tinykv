package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

var parallelAppend bool = true

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	ready := d.RaftGroup.Ready()
	appendMsg := make([]pb.Message, 0)
	afterPersistSendMsg := make([]pb.Message, 0)
	if parallelAppend {
		// Append Log并行化
		// 第一轮发送不带commit safe
		// 第二轮带上commit发送， 但是leader的log已经在上一轮持久化了 safe
		for _, msg := range ready.Messages {
			if msg.MsgType == eraftpb.MessageType_MsgAppend && msg.Snapshot == nil {
				appendMsg = append(appendMsg, msg)
			} else {
				afterPersistSendMsg = append(afterPersistSendMsg, msg)
			}
		}
		d.Send(d.ctx.trans, appendMsg)
		ready.Messages = nil
	}
	log.Debugf("{%v} hasReady {%v}", d.Tag, ready)
	res, _ := d.peer.peerStorage.SaveReadyState(&ready)
	if res != nil && !reflect.DeepEqual(res.PrevRegion, res.Region) {
		log.Infof("change region id{%v} to id{%v}", res.PrevRegion.Id, res.Region.Id)
		d.SetRegion(res.Region)
		metaStore := d.ctx.storeMeta
		metaStore.Lock()
		metaStore.regions[res.Region.Id] = res.Region
		// when preRegion is invaild(start key is "") will delete the region 1 that cause the meta corrupt
		// metaStore.regionRanges.Delete(&regionItem{res.PrevRegion})
		metaStore.regionRanges.ReplaceOrInsert(&regionItem{res.Region})
		metaStore.Unlock()
	}
	if d.IsLeader() {
		d.notifyHeartbeatScheduler(d.peerStorage.region, d.peer)
	}
	if len(ready.CommittedEntries) > 0 {
		KVWB := new(engine_util.WriteBatch)
		// check and need to remove
		if d.IsLeader() {
			cnt := 0
			for _, entry := range ready.CommittedEntries {
				if entry.EntryType == eraftpb.EntryType_EntryConfChange {
					log.Infof("node {%v} confIdx is {%v}", d.Tag, entry.Index)
					cnt++
				}
			}
			if cnt > 1 {
				panic("err ")
			}
		}
		for _, entry := range ready.CommittedEntries {
			KVWB = d.processCommittedEntries(&entry, KVWB)
			if d.stopped {
				return
			}
		}
		lastEntry := ready.CommittedEntries[len(ready.CommittedEntries)-1]
		// y.AssertTruef(lastEntry.Index == d.RaftGroup.CommitIndex(), "%d commitEntry index: %v  committed: %v", d.Tag, lastEntry.Index, d.RaftGroup.CommitIndex())
		d.peerStorage.applyState.AppliedIndex = lastEntry.Index
		if err := KVWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
			log.Panic(err)
		}
		KVWB.MustWriteToDB(d.ctx.engine.Kv)
	}
	if d.peerStorage.raftState.LastIndex < d.peerStorage.raftState.HardState.Commit || d.peerStorage.raftState.HardState.Commit < d.peerStorage.AppliedIndex() {
		log.Fatalf("Node tag{%v} save ready state lastIndex{%v} commitIdx{%v} trunIdx{%v}, applyIndex{%v}", d.peerStorage.Tag, d.peerStorage.raftState.LastIndex, d.peerStorage.raftState.HardState.Commit,
			d.peerStorage.truncatedIndex(), d.peerStorage.applyState.AppliedIndex)
	}
	if d.RaftGroup.Raft.ReadOnlyOption == raft.ReadOnlyLeaseBased {
		// follower and leader
		d.processLeaseBaseRead(ready)
	}
	if d.RaftGroup.Raft.ReadOnlyOption == raft.ReadOnlySafe {
		// only leader
		d.processReadIndex(ready)
	}

	// persist and send resp
	if parallelAppend {
		d.Send(d.ctx.trans, afterPersistSendMsg)
	} else {
		d.Send(d.ctx.trans, ready.Messages)
	}
	d.RaftGroup.Advance(ready)

}

func (d *peerMsgHandler) processLeaseBaseRead(ready raft.Ready) {
	count_ok := 0
	for _, readState := range ready.ReadStates {
		if readState.Index == 0 || readState.Index > d.peerStorage.AppliedIndex() {
			if readState.Index > d.peerStorage.AppliedIndex() {
				log.Warningf("%v is lead{%v} and apply < commit. follower read, but follwer not catch the log, done but action for it", d.Tag, ready.Lead)
			}
			requests := &raft_cmdpb.RaftCmdRequest{}
			if err := requests.Unmarshal(readState.RequestCtx); err != nil {
				log.Panic(err)
			}
			cb, ok := d.readIndexCallbacks[string(readState.RequestCtx)]
			log.Warningf("%v propose the cmd id %v is done(no action)", d.Tag, requests.CmdIdentify)
			if !ok {
				log.Panicf("cb not exist")
			}
			count_ok++
			log.Warningf("follwer read fail")
			cb.Done(ErrRespStaleCommand(d.Term()))
			delete(d.readIndexCallbacks, string(readState.RequestCtx))
			continue
		}
		if readState.Index <= d.peerStorage.AppliedIndex() {
			requests := &raft_cmdpb.RaftCmdRequest{}
			if err := requests.Unmarshal(readState.RequestCtx); err != nil {
				log.Panic(err)
			}
			var responceBatch raft_cmdpb.RaftCmdResponse
			header := new(raft_cmdpb.RaftResponseHeader)
			header.CurrentTerm = d.RaftGroup.Raft.Term
			responceBatch.Header = header
			is_snap := false
			for _, request := range requests.Requests {
				var responce raft_cmdpb.Response
				switch request.CmdType {
				case raft_cmdpb.CmdType_Get:
					if err := util.CheckKeyInRegion(request.Get.Key, d.Region()); err != nil {
						BindRespError(&responceBatch, err)
						log.Warning("request region err get")
					} else {
						val, err := engine_util.GetCF(d.ctx.engine.Kv, request.Get.Cf, request.Get.GetKey())
						if err != nil {
							val = nil
						}
						responce.CmdType = raft_cmdpb.CmdType_Get
						responce.Get = &raft_cmdpb.GetResponse{
							Value: val,
						}
					}
				case raft_cmdpb.CmdType_Snap:
					if requests.Header.RegionId != d.peerStorage.region.Id || requests.Header.RegionEpoch.Version != d.peerStorage.region.RegionEpoch.Version {
						BindRespError(&responceBatch, &util.ErrEpochNotMatch{})
						log.Warningf("request region err snap request{%v} d{%v}", requests.Header.RegionEpoch.Version, d.Region().RegionEpoch.Version)
					} else {
						responce.Snap = new(raft_cmdpb.SnapResponse)
						responce.CmdType = raft_cmdpb.CmdType_Snap
						responce.Snap.Region = new(metapb.Region)
						*responce.Snap.Region = *d.Region()
						is_snap = true
					}
				default:
					log.Panicf("not expected raft cmd type on readIndex")
				}

				responceBatch.Responses = append(responceBatch.Responses, &responce)
			}
			count_ok++
			cb, ok := d.readIndexCallbacks[string(readState.RequestCtx)]
			log.Warningf("%v propose the cmd id %v is done", d.Tag, requests.CmdIdentify)
			if !ok {
				log.Panicf("cb not exist")
			}
			if is_snap {
				cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}
			// 架构同步apply, 如果到了这里的话，提交的日志都被apply了，也就是说这里的readIndex基本都是可以done的
			//log.Warningf("%v readIndex process down", string(readState.RequestCtx))
			cb.Done(&responceBatch)
			delete(d.readIndexCallbacks, string(readState.RequestCtx))
		}
	}
	y.AssertTruef(count_ok == len(ready.ReadStates), "not match readstate_size %v and process num %v", len(ready.ReadStates), count_ok)

}
func (d *peerMsgHandler) processReadIndex(ready raft.Ready) {
	if !d.IsLeader() {
		for _, cb := range d.readIndexCallbacks {
			// channel will pause the gorutine when cb already has one resp, it's realy difficult to find
			// but fortunately, it last 10 minutes and panic
			cb.Done(ErrRespStaleCommand(d.Term()))
		}
		d.readIndexCallbacks = make(map[string]*message.Callback)
		log.Warningf("%v is step follower, abort all ReadIndex", d.Tag)
		return
	}
	log.Warningf("%v is leader, process all ReadIndex", d.Tag)
	count_ok := 0
	for _, readState := range ready.ReadStates {
		if readState.Index <= d.peerStorage.AppliedIndex() {
			requests := &raft_cmdpb.RaftCmdRequest{}
			if err := requests.Unmarshal(readState.RequestCtx); err != nil {
				log.Panic(err)
			}
			var responceBatch raft_cmdpb.RaftCmdResponse
			header := new(raft_cmdpb.RaftResponseHeader)
			header.CurrentTerm = d.RaftGroup.Raft.Term
			responceBatch.Header = header
			is_snap := false
			for _, request := range requests.Requests {
				var responce raft_cmdpb.Response
				switch request.CmdType {
				case raft_cmdpb.CmdType_Get:
					if err := util.CheckKeyInRegion(request.Get.Key, d.Region()); err != nil {
						BindRespError(&responceBatch, err)
						log.Warning("request region err get")
					} else {
						val, err := engine_util.GetCF(d.ctx.engine.Kv, request.Get.Cf, request.Get.GetKey())
						if err != nil {
							val = nil
						}
						responce.CmdType = raft_cmdpb.CmdType_Get
						responce.Get = &raft_cmdpb.GetResponse{
							Value: val,
						}
					}
				case raft_cmdpb.CmdType_Snap:
					if requests.Header.RegionId != d.peerStorage.region.Id || requests.Header.RegionEpoch.Version != d.peerStorage.region.RegionEpoch.Version {
						BindRespError(&responceBatch, &util.ErrEpochNotMatch{})
						log.Warningf("request region err snap request{%v} d{%v}", requests.Header.RegionEpoch.Version, d.Region().RegionEpoch.Version)
					} else {
						responce.Snap = new(raft_cmdpb.SnapResponse)
						responce.CmdType = raft_cmdpb.CmdType_Snap
						responce.Snap.Region = new(metapb.Region)
						*responce.Snap.Region = *d.Region()
						is_snap = true
					}
				default:
					log.Panicf("not expected raft cmd type on readIndex")
				}

				responceBatch.Responses = append(responceBatch.Responses, &responce)
			}
			count_ok++
			cb, ok := d.readIndexCallbacks[string(readState.RequestCtx)]
			log.Warningf("%v propose the cmd id %v is done", d.Tag, requests.CmdIdentify)
			if !ok {
				log.Panicf("cb not exist")
			}
			if is_snap {
				cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}
			// 架构同步apply, 如果到了这里的话，提交的日志都被apply了，也就是说这里的readIndex基本都是可以done的
			//log.Warningf("%v readIndex process down", string(readState.RequestCtx))
			cb.Done(&responceBatch)
			delete(d.readIndexCallbacks, string(readState.RequestCtx))
		}
	}
	y.AssertTruef(count_ok == len(ready.ReadStates), "not match readstate_size %v and process num %v apply index %v, commit index{%v} readstate{%v}", len(ready.ReadStates), count_ok, d.peerStorage.AppliedIndex(), d.peerStorage.raftState.HardState.Commit, ready.ReadStates)

}
func (d *peerMsgHandler) processCommittedEntries(entry *eraftpb.Entry, KVwb *engine_util.WriteBatch) *engine_util.WriteBatch {
	requests := &raft_cmdpb.RaftCmdRequest{}
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := &eraftpb.ConfChange{}
		if err := cc.Unmarshal(entry.Data); err != nil {
			log.Panic(err)
		}
		return d.processConfChange(entry, cc, KVwb)
	}
	if err := requests.Unmarshal(entry.Data); err != nil {
		log.Panic(err)
	}
	if requests.AdminRequest != nil {
		return d.processAdminRequest(entry, requests, KVwb)
	}
	return d.processRequest(entry, requests, KVwb)

}

func (d *peerMsgHandler) processConfChange(entry *eraftpb.Entry, cc *eraftpb.ConfChange, KVwb *engine_util.WriteBatch) *engine_util.WriteBatch {
	cmd := new(raft_cmdpb.RaftCmdRequest)
	if err := cmd.Unmarshal(cc.Context); err != nil {
		panic(err)
	}
	request := cmd.AdminRequest
	changeNodeId := request.GetChangePeer().Peer.Id
	if d.IsLeader() {
		log.Infof("leaderNode{%v} changeConf{%v {%v}}", d.Tag, cc.ChangeType, cc.NodeId)
		if d.RaftGroup.Raft.PendingConfIndex != entry.Index {
			log.Panicf("pendConfIndex{%v} entryIdx{%v}", d.RaftGroup.Raft.PendingConfIndex, entry.Index)
		}
	}
	if request.GetChangePeer().ChangeType == eraftpb.ConfChangeType_AddNode {
		log.Infof("{%v} add node{%v}", d.Tag, changeNodeId)
		for _, peer := range d.peerStorage.region.Peers {
			if peer.Id == changeNodeId {
				log.Infof("node{%v} has existed, no need to add", changeNodeId)
				return KVwb
			}
		}
		d.peerStorage.region.RegionEpoch.ConfVer++
		d.peerStorage.region.Peers = append(d.peerStorage.region.Peers, request.ChangePeer.Peer)
		log.Infof("{%v} increase {%v} Peers{%v}", d.Tag, d.peerStorage.region.RegionEpoch.ConfVer, d.peerStorage.region.Peers)
		d.RaftGroup.ApplyConfChange(eraftpb.ConfChange{ChangeType: eraftpb.ConfChangeType_AddNode, NodeId: changeNodeId})
		meta.WriteRegionState(KVwb, d.peerStorage.region, rspb.PeerState_Normal)

		metaStore := d.ctx.storeMeta
		metaStore.Lock()
		log.Infof("%v insert %v", d.storeID(), d.Region())
		metaStore.regions[d.regionId] = d.peerStorage.region
		metaStore.regionRanges.ReplaceOrInsert(&regionItem{d.peerStorage.region})
		metaStore.Unlock()
		d.insertPeerCache(request.ChangePeer.Peer)
		// d.PeersStartPendingTime[changeNodeId] = time.Now()
	}
	if request.GetChangePeer().ChangeType == eraftpb.ConfChangeType_RemoveNode {
		log.Infof("{%v} remove node{%v}", d.Tag, changeNodeId)
		exit_flag := true
		if len(d.peerStorage.region.Peers) == 2 {
			log.Errorf("err only have 2 nodes")
		}
		for _, peer := range d.peerStorage.region.Peers {
			if peer.Id == changeNodeId {
				exit_flag = false
				break
			}
		}
		if exit_flag {
			log.Infof("node{%v} has no existed, need to break", changeNodeId)
			return KVwb
		}

		d.RaftGroup.ApplyConfChange(eraftpb.ConfChange{ChangeType: eraftpb.ConfChangeType_RemoveNode, NodeId: changeNodeId})
		newPeers := make([]*metapb.Peer, 0)
		for _, Peer := range d.peerStorage.region.Peers {
			if Peer.Id != changeNodeId {
				newPeers = append(newPeers, Peer)
			}
		}
		d.peerStorage.region.RegionEpoch.ConfVer++
		log.Infof("{%v} increase {%v} Peers{%v}", d.Tag, d.peerStorage.region.RegionEpoch.ConfVer, d.peerStorage.region.Peers)
		d.peerStorage.region.Peers = newPeers
		meta.WriteRegionState(KVwb, d.peerStorage.region, rspb.PeerState_Normal)

		metaStore := d.ctx.storeMeta
		metaStore.Lock()
		log.Infof("%v insert %v", d.storeID(), d.Region())
		metaStore.regionRanges.ReplaceOrInsert(&regionItem{d.peerStorage.region})
		metaStore.regions[d.regionId] = d.peerStorage.region
		metaStore.Unlock()
		d.removePeerCache(changeNodeId)
		if d.storeID() == request.ChangePeer.Peer.StoreId {
			d.destroyPeer()
			log.Infof("%v is destroy peer %v", d.Tag, changeNodeId)
			return KVwb
		}
		// delete(d.PeersStartPendingTime, changeNodeId)
	}
	// localState.Region = d.peerStorage.region
	// KVwb.SetMeta(meta.RegionStateKey(d.regionId), localState)
	d.peerStorage.applyState.AppliedIndex = entry.Index
	KVwb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	KVwb.WriteToDB(d.ctx.engine.Kv)
	KVwb = new(engine_util.WriteBatch)
	resp := newResp()
	d.processCallback(entry, resp, false)
	if d.IsLeader() {
		log.Infof("leaderNode{%v} send heartbeat", d.Tag)
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return KVwb
}

func (d *peerMsgHandler) processAdminRequest(entry *eraftpb.Entry, requests *raft_cmdpb.RaftCmdRequest, KVwb *engine_util.WriteBatch) *engine_util.WriteBatch {
	request := requests.AdminRequest
	//TODO: can apply not by the committed entry
	if request.CmdType == raft_cmdpb.AdminCmdType_Split {
		if requests.Header.RegionId != d.regionId {
			regionNotFound := &util.ErrRegionNotFound{RegionId: requests.Header.RegionId}
			d.processCallback(entry, ErrResp(regionNotFound), false)
			log.Warning(regionNotFound)
			return KVwb
		}

		if errEpochNotMatch, ok := util.CheckRegionEpoch(requests, d.Region(), true).(*util.ErrEpochNotMatch); ok {
			d.processCallback(entry, ErrResp(errEpochNotMatch), false)
			log.Warning("stale{%v}", errEpochNotMatch)
			return KVwb
		}

		if err := util.CheckKeyInRegion(request.Split.SplitKey, d.Region()); err != nil {
			d.processCallback(entry, ErrResp(err), false)
			log.Warning(err)
			return KVwb
		}

		if len(d.Region().Peers) != len(request.Split.NewPeerIds) {
			d.processCallback(entry, ErrRespStaleCommand(d.Term()), false)
			log.Warning("peers num diff")
			return KVwb
		}
		Peers := make([]*metapb.Peer, 0)
		for _, peer := range d.peerStorage.region.Peers {
			newPeer := new(metapb.Peer)
			*newPeer = *peer
			Peers = append(Peers, newPeer)
		}
		for i, peerId := range request.Split.NewPeerIds {
			Peers[i].Id = peerId
		}
		newRegion := new(metapb.Region)
		newRegion.Id = request.Split.NewRegionId
		newRegion.StartKey = request.Split.SplitKey
		newRegion.EndKey = d.peerStorage.region.EndKey
		newRegion.RegionEpoch = &metapb.RegionEpoch{ConfVer: InitEpochConfVer, Version: InitEpochVer}
		newRegion.Peers = Peers
		metaStore := d.ctx.storeMeta

		metaStore.Lock()
		// can't delete, may be stale destroy region
		log.Infof("%v insert %v, split new{%v}", d.storeID(), d.Region(), newRegion)
		// metaStore.regionRanges.Delete(&regionItem{region: d.peerStorage.region})
		d.Region().EndKey = request.Split.SplitKey
		d.peerStorage.region.RegionEpoch.Version++
		metaStore.regions[d.peerStorage.region.Id] = d.peerStorage.region
		metaStore.regions[request.Split.NewRegionId] = newRegion
		metaStore.regionRanges.ReplaceOrInsert(&regionItem{newRegion})
		metaStore.regionRanges.ReplaceOrInsert(&regionItem{d.peerStorage.region})
		metaStore.Unlock()

		newRegionState := new(rspb.RegionLocalState)
		oldRegionState := new(rspb.RegionLocalState)
		newRegionState.State = rspb.PeerState_Normal
		newRegionState.Region = newRegion
		oldRegionState.State = rspb.PeerState_Normal
		oldRegionState.Region = d.peerStorage.region

		KVwb.SetMeta(meta.RegionStateKey(request.Split.NewRegionId), newRegionState)
		KVwb.SetMeta(meta.RegionStateKey(d.peerStorage.region.Id), oldRegionState)

		d.peerStorage.applyState.AppliedIndex = entry.Index
		KVwb.SetMeta(meta.ApplyStateKey(d.peerStorage.region.Id), d.peerStorage.applyState)
		KVwb.WriteToDB(d.ctx.engine.Kv)
		KVwb = &engine_util.WriteBatch{}

		newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			panic(err)
		}
		// bugfix: when one node start, it will create its peers(ininal msg).
		// So we need to detect the already exist node in the implement. it seems that regions[] can do this
		d.ctx.router.register(newPeer)
		d.ctx.router.send(newRegion.Id, message.NewMsg(message.MsgTypeStart, nil))

		log.Warningf("Node{%v} split key{%v} oldRegion{%v} newRegion{%v}", d.Tag, request.Split.SplitKey, d.peerStorage.region.Id, request.Split.NewRegionId)
		if d.IsLeader() {
			log.Infof("leaderNode{%v} send heartbeat split", d.Tag)
			d.notifyHeartbeatScheduler(oldRegionState.Region, d.peer)
			d.notifyHeartbeatScheduler(newRegionState.Region, newPeer)
		}
		resp := newResp()
		resp.Header = &raft_cmdpb.RaftResponseHeader{}
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{newRegion, d.peerStorage.region}},
		}
		resp.AdminResponse.CmdType = raft_cmdpb.AdminCmdType_Split
		d.processCallback(entry, resp, false)
	}
	return KVwb
}
func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	log.Infof("cloneRegion{%v}", clonedRegion.StartKey)
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}
func (d *peerMsgHandler) processRequest(entry *eraftpb.Entry, request *raft_cmdpb.RaftCmdRequest, KVwb *engine_util.WriteBatch) *engine_util.WriteBatch {
	var responceBatch raft_cmdpb.RaftCmdResponse
	header := new(raft_cmdpb.RaftResponseHeader)
	header.CurrentTerm = d.RaftGroup.Raft.Term
	// TODO uuid and error
	responceBatch.Header = header
	is_snap := false
	requestRename := request
	log.Warningf("%v put id done %v", d.Tag, request.CmdIdentify)
	for _, request := range request.Requests {
		var responce raft_cmdpb.Response
		switch request.CmdType {
		case raft_cmdpb.CmdType_Get:
			if err := util.CheckKeyInRegion(request.Get.Key, d.Region()); err != nil {
				BindRespError(&responceBatch, err)
				log.Warning("request region err get")
			} else {
				d.peerStorage.applyState.AppliedIndex = entry.Index
				KVwb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
				KVwb.WriteToDB(d.ctx.engine.Kv)
				KVwb = &engine_util.WriteBatch{}
				val, err := engine_util.GetCF(d.ctx.engine.Kv, request.Get.Cf, request.Get.GetKey())
				if err != nil {
					val = nil
				}
				responce.CmdType = raft_cmdpb.CmdType_Get
				responce.Get = &raft_cmdpb.GetResponse{
					Value: val,
				}
			}
		case raft_cmdpb.CmdType_Delete:
			if err := util.CheckKeyInRegion(request.Delete.Key, d.Region()); err != nil {
				BindRespError(&responceBatch, err)
				log.Warning("request region err delete")
			} else {
				responce.CmdType = raft_cmdpb.CmdType_Delete
				d.SizeDiffHint = d.SizeDiffHint + uint64((len(request.Delete.Key) + len(request.Delete.Cf)))
				KVwb.DeleteCF(request.Delete.Cf, request.Delete.Key)
			}

		case raft_cmdpb.CmdType_Put:
			if err := util.CheckKeyInRegion(request.Put.Key, d.Region()); err != nil {
				BindRespError(&responceBatch, err)
				log.Warning("request region err put")
			} else {
				responce.CmdType = raft_cmdpb.CmdType_Put
				d.SizeDiffHint = d.SizeDiffHint + uint64((len(request.Put.Key) + len(request.Put.Value) + len(request.Put.Cf)))
				KVwb.SetCF(request.Put.Cf, request.Put.Key, request.Put.Value)
			}
		case raft_cmdpb.CmdType_Snap:
			log.Infof("request region{%v} err snap request{%v} d{%v}", d.peerStorage.region.Id, requestRename.Header.RegionEpoch.Version, d.Region().RegionEpoch.Version)
			if requestRename.Header.RegionId != d.peerStorage.region.Id || requestRename.Header.RegionEpoch.Version != d.peerStorage.region.RegionEpoch.Version {
				BindRespError(&responceBatch, &util.ErrEpochNotMatch{})
				log.Warningf("request region err snap request{%v} d{%v}", requestRename.Header.RegionEpoch.Version, d.Region().RegionEpoch.Version)
			} else {
				d.peerStorage.applyState.AppliedIndex = entry.Index
				KVwb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
				KVwb.WriteToDB(d.ctx.engine.Kv)
				KVwb = &engine_util.WriteBatch{}
				responce.Snap = new(raft_cmdpb.SnapResponse)
				responce.CmdType = raft_cmdpb.CmdType_Snap
				responce.Snap.Region = new(metapb.Region)
				*responce.Snap.Region = *d.Region()
				is_snap = true
			}
			// TODO:2C
		}
		responceBatch.Responses = append(responceBatch.Responses, &responce)
	}
	d.processCallback(entry, &responceBatch, is_snap)
	return KVwb

}
func (d *peerMsgHandler) processCallback(entry *eraftpb.Entry, responce *raft_cmdpb.RaftCmdResponse, snapshot bool) {
	for index, proposal := range d.proposals {
		if proposal.index == entry.Index && proposal.term == entry.Term {
			if snapshot {
				proposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}
			log.Warningf("%v cb done, propose index %v term %v", d.Tag, proposal.index, proposal.term)
			proposal.cb.Done(responce)
			for i := 0; i < index; i++ {
				// stale command and responce err
				d.proposals[i].cb.Done(ErrResp(&util.ErrStaleCommand{}))
			}
			d.proposals = d.proposals[index+1:]
			break
		}
	}
}
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

var id uint64 = 0

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// ReadIndex
	msg.CmdIdentify = id + 1
	id++
	// Your Code Here (2B).
	proposeData, err := msg.Marshal()
	if err != nil {
		log.Panic(err)
	}
	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader {
		log.Infof("admin request change reach change leader{%v}", msg.GetAdminRequest().GetTransferLeader().Peer.Id)
		d.RaftGroup.TransferLeader(msg.GetAdminRequest().GetTransferLeader().Peer.Id)
		resp := newResp()
		resp.AdminResponse = new(raft_cmdpb.AdminResponse)
		resp.AdminResponse.CmdType = raft_cmdpb.AdminCmdType_TransferLeader
		cb.Done(resp)
		return
	}
	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
		context, err := msg.Marshal()
		if err != nil {
			panic("err marshal")
		}
		if len(d.peerStorage.region.Peers) == 2 && msg.AdminRequest.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode && msg.AdminRequest.ChangePeer.Peer.Id == d.PeerId() {
			var TargetNodeId uint64 = 0
			for _, peer := range d.peerStorage.region.Peers {
				if peer.Id != d.PeerId() {
					TargetNodeId = peer.Id
					break
				}
			}
			log.Errorf("[defend]leaderNode{%v} changeLeader to {%v}", d.PeerId(), TargetNodeId)
			y.Assert(TargetNodeId != 0)
			d.RaftGroup.TransferLeader(TargetNodeId)
			cb.Done(ErrResp(&util.ErrNotLeader{}))
			return
		}
		d.proposals = append(d.proposals, &proposal{d.RaftGroup.Raft.RaftLog.LastIndex() + 1, d.RaftGroup.Raft.Term, cb})
		d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{ChangeType: msg.AdminRequest.ChangePeer.GetChangeType(), NodeId: msg.AdminRequest.ChangePeer.Peer.Id, Context: context})
		return
	}
	// ReadIndex
	var readOnlyRequests = false // for the adminRequest type, it must be false defaultly
	flag := true
	for _, msg := range msg.Requests {
		if msg.CmdType != raft_cmdpb.CmdType_Get && msg.CmdType != raft_cmdpb.CmdType_Snap {
			flag = false
		}
	}
	if flag && msg.AdminRequest == nil {
		readOnlyRequests = true
	}
	if readOnlyRequests {
		//log.Warningf("%v readIndex process", string(proposeData))
		ent := pb.Entry{Data: proposeData}
		if err := d.RaftGroup.Step(pb.Message{MsgType: eraftpb.MessageType_MsgReadIndex, Entries: []*pb.Entry{&ent}}); err != nil {
			panic(err)
		}
		if _, ok := d.readIndexCallbacks[string(proposeData)]; ok {
			log.Panicf("req duplicate")
		}
		d.readIndexCallbacks[string(proposeData)] = cb
		log.Warningf("%v propose the cmd id %v", d.Tag, msg.CmdIdentify)
		return
	} else {
		log.Warningf("%v put id %v", d.Tag, msg.CmdIdentify)
	}
	// endReadIndex
	d.proposals = append(d.proposals, &proposal{d.RaftGroup.Raft.RaftLog.LastIndex() + 1, d.RaftGroup.Raft.Term, cb})
	d.RaftGroup.Propose(proposeData)
}

func newResp() *raft_cmdpb.RaftCmdResponse {
	resp := new(raft_cmdpb.RaftCmdResponse)
	resp.Header = new(raft_cmdpb.RaftResponseHeader)
	return resp
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	log.Infof("%v and regionId{%v} cur meta{%v}", d.Region(), meta.regions, meta.regionRanges)
	// when one store has two nodes, and detected the stale epoch it will cause panic here, so it maybe need to change ERROR info
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		log.Panicf(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		log.Panicf(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	log.Infof("region{%v}raft send the command to compact logs to {%v}", regionID, compactIdx)
	// compact logs don't impact the kv store, so it's ok to proces it immediately
	KVwb := new(engine_util.WriteBatch)
	compactLog := request.AdminRequest.CompactLog
	log.Infof("d.Tag{%v} leader{%v} admin request reach compactlog{%v} compactterm{%v}, now TrunIdex{%v}", d.Tag, d.IsLeader(), compactLog.CompactIndex, compactLog.CompactTerm, d.peerStorage.truncatedIndex())
	// d.peerStorage.applyState.AppliedIndex = compactLog.CompactIndex
	if d.peerStorage.applyState.TruncatedState.Index > compactLog.CompactIndex {
		log.Warningf("compact request is stale!")
		return
	}
	d.peerStorage.applyState.TruncatedState.Index = compactLog.CompactIndex
	d.peerStorage.applyState.TruncatedState.Term = compactLog.CompactTerm
	KVwb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	KVwb.WriteToDB(d.ctx.engine.Kv)
	d.ScheduleCompactLog(compactLog.CompactIndex)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	//if diffSize Hint not enable, it only access until approximateSize != nil.
	//So when many clients put kv, it will be very slow
	//(test scan need scan all region kv), not split the region.
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		log.Infof("%v diff size %v", d.Tag, d.SizeDiffHint)
		return
	}
	log.Infof("%v send spilt check task(diff size %v)", d.Tag, d.SizeDiffHint)
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
