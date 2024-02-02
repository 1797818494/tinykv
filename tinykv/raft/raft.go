// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"bytes"
	"errors"
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next  uint64
	RecentActive bool
	SendBuffer   *SendBuffer
}

type Raft struct {
	id uint64

	Term    uint64
	Vote    uint64
	TickNum int

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead          uint64
	peers         []uint64
	checkQuorum   bool
	maxBufferSize int
	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	electionRadomTimeOut int
	heartbeatElapsed     int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	readIndex        *readIndex
	pendingReadIndex []*pb.Message
	ReadOnlyOption   ReadOnlyOption
	leaderLease      bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	log.SetLevel(log.LOG_LEVEL_DEBUG)
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// log.SetLevel(log.LOG_LEVEL_DEBUG)
	var raft Raft
	raft.id = c.ID
	raft.RaftLog = newLog(c.Storage)
	raft.Term = raft.RaftLog.curTerm
	raft.Vote = raft.RaftLog.Vote
	raft.Prs = make(map[uint64]*Progress)
	raft.votes = make(map[uint64]bool)
	raft.peers = c.peers
	raft.TickNum = 0
	raft.checkQuorum = true                  // open the checkQuorum feature (write here and not write the config because maintain the test code)
	raft.maxBufferSize = 1000                // like above
	raft.ReadOnlyOption = ReadOnlyLeaseBased // like above
	raft.leaderLease = true                  // like above
	raft.electionElapsed = 0
	raft.electionTimeout = c.ElectionTick
	raft.heartbeatElapsed = 0
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.RaftLog.appliedTo(c.Applied)
	raft.ResetElectionTime()
	raft.readIndex = newReadIndex()
	for _, p := range raft.peers {
		raft.Prs[p] = &Progress{Match: 0, Next: 1, RecentActive: false, SendBuffer: NewSendBuffer(raft.maxBufferSize)}
	}
	// Your Code Here (2A).
	log.Infof("raft{%v} new succeed important state: vote{%v} term{%v} commit{%v} peers{%v} applied{%v} stabled{%v} dumyIndex{%v}", raft.id, raft.Vote,
		raft.Term, raft.RaftLog.committed, raft.peers, raft.RaftLog.applied, raft.RaftLog.stabled, raft.RaftLog.dummyIndex)
	return &raft
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) checkQuorumActive() bool {
	var act int

	for id := range r.Prs {
		if id == r.id { // self is always active
			act++
			continue
		}

		if r.Prs[id].RecentActive {
			act++
		}

		r.Prs[id].RecentActive = false
	}

	return act >= len(r.peers)/2+1
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err == nil && r.RaftLog.dummyIndex <= r.Prs[to].Next {
		m := pb.Message{}
		m.To = to
		m.From = r.id
		ents := r.RaftLog.getEntries(prevLogIndex+1, 0)
		m.MsgType = pb.MessageType_MsgAppend

		m.Term = r.Term
		m.Index = prevLogIndex
		m.LogTerm = prevLogTerm
		m.Entries = make([]*pb.Entry, 0)
		m.Entries = append(m.Entries, changeToPoint(ents)...)
		m.Commit = r.RaftLog.committed
		log.Debugf("Node{%v} sendAppend Next{%v} len(%v) to{%v}", r.id, prevLogIndex, len(r.RaftLog.entries), to)
		// pipeline optimizer
		r.Prs[to].Next = r.RaftLog.LastIndex() + 1
		r.send(m)
		return true
	} else {
		if !r.Prs[to].RecentActive {
			log.Warningf("%v skip the snapshot since it is not recentactive", to)
			return false
		}
		var err error
		var snap pb.Snapshot
		snap, err = r.RaftLog.storage.Snapshot()

		m := pb.Message{}
		m.To = to
		m.From = r.id
		m.MsgType = pb.MessageType_MsgSnapshot
		m.Term = r.Term
		if err != nil {
			// 异步snap，然后通知？
			log.Infof("Node{%v} aysnc snap", r.id)
			return false
		}
		// r.RaftLog.pendingSnapshot = &snap
		m.Index = snap.Metadata.Index
		m.LogTerm = snap.Metadata.Term
		m.Commit = r.RaftLog.committed
		m.Snapshot = &snap
		r.Prs[to].Next = snap.Metadata.Index + 1
		log.Infof("Node{%v} sendsnap to{%v} snapshotIndex{%v}", r.id, to, m.Index)
		r.send(m)
		return true
	}
}
func (r *Raft) appendEntry(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Index = lastIndex + uint64(i) + 1
		entries[i].Term = r.Term
	}
	r.RaftLog.appendNewEntry(entries)
	// for _, entry := range r.RaftLog.allEntries() {
	// 	log.Infof("Node{%v} sendAppend term{%v} index{%v}", r.id, entry.Term, entry.Index)
	// }
}
func (r *Raft) send(m pb.Message) {
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

func changeToPoint(ents []pb.Entry) []*pb.Entry {
	entsPoint := []*pb.Entry{}
	for i := range ents {
		entsPoint = append(entsPoint, &ents[i])
	}
	return entsPoint
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64, ctx []byte) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  commit,
		Context: ctx,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if !r.isInGroup() {
		return
	}
	r.TickNum++
	if r.TickNum >= r.electionElapsed+r.electionRadomTimeOut {
		if r.checkQuorum {
			r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgCheckQuorum})
		}
		if r.State == StateFollower || r.State == StateCandidate {
			// log.Infof("Node{%v} become candidate term{%v}, ticker{%v}", r.id, r.Term, r.TickNum)
			msg_hup := pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id, Term: r.Term}
			r.Step(msg_hup)
		}
		if r.State == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}
	if r.TickNum >= r.heartbeatElapsed+r.heartbeatTimeout {
		if r.State == StateLeader {
			log.Infof("Node{%v} start hearbeat term{%v}, ticker{%v}", r.id, r.Term, r.TickNum)
			msg_heart := pb.Message{MsgType: pb.MessageType_MsgBeat, To: r.id,
				From: r.id, Term: r.Term, Commit: r.RaftLog.committed}
			r.Step(msg_heart)
		}
	}
}
func (r *Raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// r.Vote = lead
	if term > r.Term {
		r.Vote = None
	}
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.abortLeaderTransfer()
	r.PendingConfIndex = 0
	r.readIndex = newReadIndex()

	if r.ReadOnlyOption == ReadOnlyLeaseBased {
		for _, message := range r.pendingReadIndex {
			r.readIndex.readState = append(r.readIndex.readState, ReadState{0, message.Entries[0].Data})
		}
	}
	r.pendingReadIndex = make([]*pb.Message, 0)
	r.ResetElectionTime()
	log.Debugf("Node{%v} become follower term{%v}, ticker{%v}", r.id, r.Term, r.TickNum)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.State = StateCandidate
	r.Lead = None
	log.Debugf("Node{%v} beecome_candiate in term{%v} tick{%v}", r.id, r.Term, r.TickNum)
	r.ResetElectionTime()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Infof("node{%v} become leader in term{%v}", r.id, r.Term)
	r.Lead = r.id
	r.State = StateLeader
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		r.Prs[id].Match = 0
		r.Prs[id].RecentActive = false
		r.Prs[id].SendBuffer.Reset()
	}
	r.pendingReadIndex = make([]*pb.Message, 0)
	r.readIndex = newReadIndex()
	r.pendingConfCheck()
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok && len(r.Prs) != 0 {
		log.Infof("%d do not exist and have other peers return, term %d, Prs %+v\n", r.id, r.Term, r.Prs)
		return nil
	}
	switch r.State {
	case StateFollower:
		if m.MsgType == pb.MessageType_MsgHup {
			r.handleMsgUp()
		}
		if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		}
		if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleRequestVote(m)
		}
		if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.handleHeartbeat(m)
		}
		if m.MsgType == pb.MessageType_MsgSnapshot {
			r.handleSnapshot(m)
		}
		if m.MsgType == pb.MessageType_MsgTimeoutNow {
			r.handleMsgTimeOut(m)
		}
		if m.MsgType == pb.MessageType_MsgTransferLeader {
			r.handleTransferNotLeader(m)
		}
	case StateCandidate:
		if m.MsgType == pb.MessageType_MsgHup {
			r.handleMsgUp()
		}
		if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		}
		if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleRequestVote(m)
		}
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			r.handleRequestVoteResponce(m)
		}
		if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.handleHeartbeat(m)
		}
		if m.MsgType == pb.MessageType_MsgReadIndex {
			r.handleFollowerRead(m)
		}
		if m.MsgType == pb.MessageType_MsgReadIndexResp {
			r.handleFollowerReadResp(m)
		}
	case StateLeader:
		if m.MsgType == pb.MessageType_MsgCheckQuorum {
			if !r.checkQuorumActive() {
				log.Warningf("%v stepped down to follower since quorum is not active", r.id)
				r.becomeFollower(r.Term, None)
			}
		}
		if m.MsgType == pb.MessageType_MsgBeat {
			r.broadcastHeartBeat()
		}
		if m.MsgType == pb.MessageType_MsgPropose {
			r.handleMsgPropose(m)
		}
		if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		}
		if m.MsgType == pb.MessageType_MsgAppendResponse {
			r.handleAppendResponse(m)
		}
		if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleRequestVote(m)
		}
		if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
			r.handleHeartbeatResponse(m)
		}
		if m.MsgType == pb.MessageType_MsgTransferLeader {
			r.handleTransfer(m)
		}
		if m.MsgType == pb.MessageType_MsgReadIndex {
			r.handleReadIndex(m)
		}

	}
	return nil
}

func (r *Raft) handleFollowerReadResp(m pb.Message) {
	if len(m.Entries) != 1 {
		log.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
		return
	}
	r.readIndex.readState = append(r.readIndex.readState, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
}
func (r *Raft) handleFollowerRead(m pb.Message) {
	if r.Lead == None {
		log.Errorf("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
		return
	}
	m.To = r.Lead
	r.send(m)
}
func (r *Raft) handleReadIndex(m pb.Message) {
	// commited term and currentTerm
	commit := r.RaftLog.committed
	commit_term, err := r.RaftLog.Term(commit)
	if err != nil {
		if r.RaftLog.pendingSnapshot == nil {
			log.Panicf("%v is crash because can't get the term of last commit log", r.id)
		}
		commit_term = r.RaftLog.pendingSnapshot.Metadata.Term
		log.Errorf("use snapshot term")
	}
	if r.Term == commit_term {
		r.processReadIndex(m)
	} else {
		log.Warningf("after commit one log(no-op)")
		r.pendingReadIndex = append(r.pendingReadIndex, &m) // after commit one log(no-op)
	}
}
func (r *Raft) processReadIndex(m pb.Message) {
	//log.Warningf("%v raft leader process ReadIndex", m.Entries[0].Data)
	if len(r.peers) != 1 {
		switch r.ReadOnlyOption {
		case ReadOnlySafe:
			log.Warningf("%v add request committed %v into readIndex", r.id, r.RaftLog.committed)
			r.readIndex.addRequest(r.RaftLog.committed, m)
			// 广播消息出去，其中消息的CTX是该读请求的唯一标识
			// 在应答是Context要原样返回，将使用这个ctx操作readOnly相关数据
			r.bcastHeartbeatWithCtx(m.Entries[0].Data)
		case ReadOnlyLeaseBased:
			// Lease机制需要同时启动checkQuorum
			var commit_id uint64 = 0
			if r.checkQuorum {
				commit_id = r.RaftLog.committed
			}
			if m.From == None || m.From == r.id { // from local member
				r.readIndex.readState = append(r.readIndex.readState, ReadState{Index: r.RaftLog.committed, RequestCtx: m.Entries[0].Data})
			} else {
				r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgReadIndexResp, Index: commit_id, Entries: m.Entries})
			}
		}
	} else {
		r.readIndex.readState = append(r.readIndex.readState, ReadState{r.RaftLog.committed, m.Entries[0].Data})
	}
}
func (r *Raft) bcastHeartbeatWithCtx(ctx []byte) {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id, ctx)
	}
	if len(r.Prs) == 1 {
		rss := r.readIndex.advance(pb.Message{Context: ctx})
		if len(rss) > 0 {
			log.Warningf("%v advance ReadIndex rssnum %v", r.id, len(rss))
		}
		for _, rs := range rss { // 遍历准备被丢弃的readindex状态
			req := rs.req
			if req.From == None || req.From == r.id { // from local member
				// 如果来自本地
				r.readIndex.readState = append(r.readIndex.readState, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
			} else {
				// 否则就是来自外部，需要应答(TODO follower read)
				log.Panicf("no implement")
			}
		}
	}
}

func (r *Raft) handleTransferNotLeader(m pb.Message) {
	if r.id == m.From {
		log.Infof("the follower{%v} transfer and msgup", r.id)
		r.handleMsgUpTransfer()
	} else {
		log.Infof("the follower{%v} transfer to leadr{%v} targetId{%v}", r.id, r.Lead, m.From)
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: m.From, To: r.Lead})
	}
	r.Lead = m.From
}
func (r *Raft) handleMsgTimeOut(m pb.Message) {
	if !r.isInGroup() {
		log.Warningf("node{%v} is not in loop", r.id)
		return
	}
	if m.Term == r.Term {
		r.handleMsgUpTransfer()
	} else {
		log.Warning("the follower is stale")
	}
}
func (r *Raft) handleTransfer(m pb.Message) {
	log.Debugf("Node{%v} received the transfer and target node{%v}", r.id, m.From)
	targetId := m.From
	lastTarget := r.leadTransferee
	if lastTarget != None {
		if lastTarget == targetId {
			log.Infof("same target {%v}", targetId)
			return
		}
		r.abortLeaderTransfer()
		log.Infof("target {%v} abort", lastTarget)
	}
	if targetId == r.id {
		return
	}
	if _, ok := r.Prs[targetId]; !ok {
		log.Warningf("peer{%v} no exist", targetId)
		return
	}
	r.leadTransferee = targetId
	if r.Prs[targetId].Match == r.RaftLog.LastIndex() {
		msg := pb.Message{MsgType: pb.MessageType_MsgTimeoutNow, To: targetId, From: r.id, Term: r.Term}
		r.msgs = append(r.msgs, msg)
		log.Info("node{%v} become_follower and lead{%v}", r.id, m.From)
	} else {
		log.Info("node{%v} not update expect{%v} but{%v}", m.From, r.RaftLog.LastIndex(), r.Prs[targetId].Match)
		r.transfeeHelper(targetId)
	}
	// r.becomeFollower(r.Term, m.From)
}
func (r *Raft) transfeeHelper(targetId uint64) {
	r.sendAppend(targetId)
}
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	r.Prs[m.From].RecentActive = true
	if r.Prs[m.From].SendBuffer.Full() {
		// when buffer is full, we should free one slot sendbuffer to append the log(one  RTT)
		r.Prs[m.From].SendBuffer.FreeFirstOne()
	}
	if m.Reject {
		// heartbeat 被拒绝的原因只可能是对方节点的 Term 更大
		r.becomeFollower(m.Term, None)
	} else {
		// 心跳同步成功

		// 检查该节点的日志是不是和自己是同步的，由于有些节点断开连接并又恢复了链接
		// 因此 leader 需要及时向这些节点同步日志
		log.Debug("leader{%v}  send msg to %v match %v next %v lastlog%v", r.id, m.From, r.Prs[m.From].Match, r.Prs[m.From].Next, r.RaftLog.LastIndex())
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}

	// ReadIndex process
	// 收到应答调用recvAck函数返回当前针对该消息已经应答的节点数量
	if m.Context == nil {
		return
	}
	ackCount := r.readIndex.recvAck(m.From, m.Context)
	// +1 for leader
	if len(ackCount)+1 < len(r.peers)/2+1 {
		// 小于集群半数以上就返回不往下走了
		log.Debugf("%v ackCount not reach %v", len(ackCount)+1, len(r.peers)/2+1)
		return
	}

	// 调用advance函数尝试丢弃已经被确认的read index状态
	rss := r.readIndex.advance(m)
	if len(rss) > 0 {
		log.Warningf("%v advance ReadIndex rssnum %v", r.id, len(rss))
	}
	for _, rs := range rss { // 遍历准备被丢弃的readindex状态
		req := rs.req
		if req.From == None || req.From == r.id { // from local member
			// 如果来自本地
			r.readIndex.readState = append(r.readIndex.readState, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
		} else {
			// 否则就是来自外部，需要应答(TODO follower read)
			log.Panicf("no implement")
		}
	}
}

func (r *Raft) broadcastHeartBeat() {
	lastCtx := r.readIndex.lastPendingRequestCtx()
	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		if len(lastCtx) == 0 {
			r.sendHeartbeat(id, nil)
		} else {
			log.Warningf("lastCtx heartbeat")
			r.sendHeartbeat(id, []byte(lastCtx))
		}

	}
	if len(r.peers) == 1 && len(lastCtx) != 0 {
		rss := r.readIndex.advance(pb.Message{Context: []byte(lastCtx)})
		if len(rss) > 0 {
			log.Warningf("%v advance ReadIndex rssnum %v", r.id, len(rss))
		}
		for _, rs := range rss { // 遍历准备被丢弃的readindex状态
			req := rs.req
			if req.From == None || req.From == r.id { // from local member
				// 如果来自本地
				r.readIndex.readState = append(r.readIndex.readState, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
			} else {
				// 否则就是来自外部，需要应答(TODO follower read)
				log.Panicf("no implement")
			}
		}
	}
	r.ResetHeartTime()
}
func (r *Raft) handleRequestVoteResponce(m pb.Message) {
	log.Debugf("Node{%v} from Node{%v} reject{%v}", m.To, m.From, m.Reject)
	// log.Infof("node{%v} receive from Node{%v}", m.To, m.From)
	r.votes[m.From] = !m.Reject
	count := 0
	for _, agree := range r.votes {
		if agree {
			count++
		}
	}
	if m.Reject {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
		if len(r.votes)-count > len(r.peers)/2 {
			r.becomeFollower(r.Term, None)
		}
	} else {
		if count > len(r.peers)/2 {
			r.becomeLeader()
		}
	}
}

func (r *Raft) handleMsgUpTransfer() {
	ents := r.RaftLog.getEntries(r.RaftLog.applied+1, r.RaftLog.committed+1)
	cnt := 0
	for _, ent := range ents {
		if ent.EntryType == pb.EntryType_EntryConfChange {
			cnt++
		}
	}
	if cnt > 0 && r.RaftLog.committed > r.RaftLog.applied {
		log.Errorf("follower become candidate should wait the logs which contain the config log applied")
		return
	}
	r.becomeCandidate()
	if len(r.peers) == 1 {
		r.becomeLeader()
		return
	}
	// r.Step(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: r.id, Reject: false, Term: r.Term})
	for _, peer := range r.peers {
		if peer != r.id {
			ctx := []byte("transferType")
			request_vote_msg := pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: peer, Term: r.Term, LogTerm: r.RaftLog.LastTerm(),
				Index: r.RaftLog.LastIndex(), Context: ctx}
			r.msgs = append(r.msgs, request_vote_msg)
		}
	}
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true

}
func (r *Raft) handleMsgUp() {
	ents := r.RaftLog.getEntries(r.RaftLog.applied+1, r.RaftLog.committed+1)
	cnt := 0
	for _, ent := range ents {
		if ent.EntryType == pb.EntryType_EntryConfChange {
			cnt++
		}
	}
	if cnt > 0 && r.RaftLog.committed > r.RaftLog.applied {
		log.Errorf("follower become candidate should wait the logs which contain the config log applied")
		return
	}
	r.becomeCandidate()
	if len(r.peers) == 1 {
		r.becomeLeader()
		return
	}
	// r.Step(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: r.id, Reject: false, Term: r.Term})
	for _, peer := range r.peers {
		if peer != r.id {
			request_vote_msg := pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: peer, Term: r.Term, LogTerm: r.RaftLog.LastTerm(),
				Index: r.RaftLog.LastIndex()}
			r.msgs = append(r.msgs, request_vote_msg)
		}
	}
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true

}
func (r *Raft) handleMsgPropose(m pb.Message) {
	if r.leadTransferee != None {
		log.Info("target{%v} is stopped the propose", r.leadTransferee)
		return
	}
	for i, entry := range m.Entries {
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex > r.RaftLog.applied {
				m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
				log.Warning("{%v}", *m.Entries[i])
			} else {
				r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(i) + 1
			}
		}
	}
	r.appendEntry(m.Entries)
	log.Infof("Node{%v} append newlog and reach len{%v}", r.id, len(r.RaftLog.allEntries()))
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	if len(r.Prs) == 1 {
		r.RaftLog.commit(r.RaftLog.LastIndex())
	} else {
		r.broadcastAppendEntry()
	}
}
func (r *Raft) isInGroup() bool {
	_, ok := r.Prs[r.id]
	return ok
}
func (pr *Progress) maybeUpdate(n uint64) bool {
	var update bool
	if pr.Match < n {
		pr.Match = n
		pr.Next = pr.Match + 1
		update = true
	}
	return update
}
func (r *Raft) handleAppendResponse(m pb.Message) {
	r.Prs[m.From].RecentActive = true
	if m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		} else {
			r.Prs[m.From].Next = m.Index + 1
			r.sendAppend(m.From)
		}
		return
	}
	// free to the appendResp index approximately
	r.Prs[m.From].SendBuffer.FreeTo(m.Index)
	if r.Prs[m.From].maybeUpdate(m.Index) {
		if r.maybeCommit() {
			// for ReadIndex
			for _, pend_m := range r.pendingReadIndex {
				r.processReadIndex(*pend_m)
			}
			r.pendingReadIndex = make([]*pb.Message, 0)
			r.broadcastAppendEntry()
		}
	}
	if m.From == r.leadTransferee && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		msg := pb.Message{MsgType: pb.MessageType_MsgTimeoutNow, To: m.From, From: r.id, Term: r.Term}
		r.msgs = append(r.msgs, msg)
	}
}
func (r *Raft) maybeCommit() bool {
	matchArray := make(uint64Slice, 0)
	for _, progress := range r.Prs {
		matchArray = append(matchArray, progress.Match)
	}
	// 获取所有节点 match 的中位数，就是被大多数节点复制的日志索引
	sort.Sort(sort.Reverse(matchArray))
	majority := len(r.Prs) / 2
	toCommitIndex := matchArray[majority]
	// 检查是否可以提交 toCommitIndex
	return r.RaftLog.maybeCommit(toCommitIndex, r.Term)
}
func (r *Raft) pendingConfCheck() {
	ents := r.RaftLog.getEntries(r.RaftLog.committed+1, 0)
	cnt := 0
	var idx uint64
	for _, ent := range ents {
		if ent.EntryType == pb.EntryType_EntryConfChange {
			cnt++
			idx = ent.Index
			log.Infof("node{%v} confIdx{%v}", r.id, idx)
		}
	}
	if cnt > 1 {
		log.Panicf("Node{%v} pendingConf{%v} > 1", r.id, cnt)
	}
	if cnt == 0 {
		r.PendingConfIndex = 0
	}
	if cnt == 1 {
		r.PendingConfIndex = idx
	}
}
func (r *Raft) broadcastAppendEntry() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		if r.Prs[id].SendBuffer.Full() {
			log.Warningf("node{%v}sendbuffer is full, skip to sendAppend to it", id)
			continue
		}
		r.sendAppend(id)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	appendEntryResp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	appendEntryResp.Reject = true

	if m.Term < r.Term {
		r.msgs = append(r.msgs, appendEntryResp)
		return
	}
	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm
	if prevLogIndex <= r.RaftLog.LastIndex() {
		if _, err := r.RaftLog.Term(prevLogIndex); err != nil {
			r.msgs = append(r.msgs, appendEntryResp)
			return
		}
	}
	r.becomeFollower(m.Term, m.From)
	if prevLogIndex > r.RaftLog.LastIndex() || r.RaftLog.TermNoErr(prevLogIndex) != prevLogTerm {
		appendEntryResp.Index = r.RaftLog.LastIndex()
		if prevLogIndex <= r.RaftLog.LastIndex() {
			conflictTerm := r.RaftLog.TermNoErr(prevLogIndex)
			// 从preLogIndex 往后面找()
			firstIndex, errSnap := r.RaftLog.storage.FirstIndex()
			if errSnap != nil {
				log.Panicf("first index not exist becaues the snap")
			}
			for i := prevLogIndex; i >= firstIndex; i-- {
				if r.RaftLog.TermNoErr(i) != conflictTerm {
					break
				}
				appendEntryResp.Index = i - 1
			}
		}
		log.Debugf("Node{%v} appendfail idx{%v}", r.id, appendEntryResp.Index)
	} else {
		if len(m.Entries) > 0 {
			idx, newLogIndex := m.Index+1, m.Index+1
			for ; idx <= r.RaftLog.LastIndex() && idx <= m.Entries[len(m.Entries)-1].Index; idx++ {
				term, err := r.RaftLog.Term(idx)
				if err != nil {
					r.msgs = append(r.msgs, appendEntryResp)
					return
				}
				if term != m.Entries[idx-newLogIndex].Term {
					break
				}
			}

			if idx-newLogIndex != uint64(len(m.Entries)) {
				r.RaftLog.truncate(idx)
				if idx-1 < r.RaftLog.committed {
					log.Fatalf("node{%v} cut the commited log ------commit{%v} idx{%v}", r.id, r.RaftLog.committed, idx)
				}
				r.RaftLog.appendNewEntry(m.Entries[idx-newLogIndex:])
				r.RaftLog.stabled = min(r.RaftLog.stabled, idx-1)
				log.Debugf("Node{%v} appendsuccess idx{%v} len{%v}", r.id, idx, len(m.Entries[idx-newLogIndex:]))
			}
		}
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.commit(min(m.Commit, m.Index+uint64(len(m.Entries))))
		}
		appendEntryResp.Reject = false
		appendEntryResp.Index = m.Index + uint64(len(m.Entries))
		appendEntryResp.LogTerm = r.RaftLog.TermNoErr(appendEntryResp.Index)
	}
	r.msgs = append(r.msgs, appendEntryResp)

}

func (r *Raft) handleRequestVote(m pb.Message) {
	//2A
	log.Debugf("node{%v} receive requestvote from{%v}", r.id, m.From)
	voteRep := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	force := bytes.Equal([]byte("transferType"), m.Context)
	inLease := r.checkQuorum && r.Lead != None && r.TickNum < (r.electionElapsed+r.electionRadomTimeOut) && r.leaderLease && !force
	if m.Term > r.Term {
		if inLease {
			// If a server receives a RequestVote request within the minimum election timeout
			// of hearing from a current leader, it does not update its term or grant its vote
			log.Warning("%v [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
				r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term, (r.electionElapsed+r.electionRadomTimeOut)-r.TickNum)
			return
		}
		r.becomeFollower(m.Term, None)
	}
	if (m.Term > r.Term || (m.Term == r.Term && (r.Vote == None || r.Vote == m.From))) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
	} else {
		voteRep.Reject = true
	}
	r.msgs = append(r.msgs, voteRep)
}

func (r *Raft) ResetElectionTime() {
	rand.Seed(time.Now().UnixNano())
	r.electionElapsed = r.TickNum
	r.electionRadomTimeOut = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) ResetHeartTime() {
	log.Infof("Node{%v}, resetheart{%v}", r.id, r.TickNum)
	r.heartbeatElapsed = r.TickNum
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	heartBeatResp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Context: m.Context,
	}
	if r.Term > m.Term {
		heartBeatResp.Reject = true
	} else {
		r.becomeFollower(m.Term, m.From)
	}
	r.msgs = append(r.msgs, heartBeatResp)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	log.Infof("Node{%v} snapshot reach", r.id)
	resp := pb.Message{}
	resp.MsgType = pb.MessageType_MsgAppendResponse
	resp.To = m.From
	resp.From = m.To
	resp.Term = r.Term
	if m.Term < r.Term || m.Snapshot == nil || IsEmptySnap(m.Snapshot) || r.RaftLog.committed >= m.Snapshot.Metadata.Index {
		// snapshot rpc is stale
		resp.Reject = true
		resp.Index = r.RaftLog.committed
		r.msgs = append(r.msgs, resp)
		return
	}
	r.becomeFollower(m.Term, m.From)
	resp.Reject = false
	resp.Index = m.Snapshot.Metadata.Index
	r.Lead = m.From
	r.ChangLogStateWithSnap(m.Snapshot)
	r.peers = m.Snapshot.Metadata.ConfState.Nodes
	r.Prs = make(map[uint64]*Progress)
	// ADD
	r.RaftLog.entries = make([]pb.Entry, 0)
	for _, peer := range r.peers {
		r.Prs[peer] = &Progress{SendBuffer: NewSendBuffer(r.maxBufferSize)}
	}
	r.msgs = append(r.msgs, resp)
}

func (r *Raft) ChangLogStateWithSnap(snapshot *pb.Snapshot) {
	r.RaftLog.committed = snapshot.Metadata.Index
	r.RaftLog.applied = snapshot.Metadata.Index
	r.RaftLog.stabled = snapshot.Metadata.Index
	r.RaftLog.dummyIndex = snapshot.Metadata.Index + 1
	r.RaftLog.entries = make([]pb.Entry, 0)
	r.RaftLog.pendingSnapshot = snapshot
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.PendingConfIndex = 0
	log.Infof("node{%v} add node{%v}", r.id, id)
	r.peers = append(r.peers, id)
	r.Prs[id] = new(Progress)
	r.Prs[id].Match = 0
	r.Prs[id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[id].SendBuffer = NewSendBuffer(r.maxBufferSize)
	r.Prs[id].RecentActive = false
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	r.PendingConfIndex = 0
	log.Infof("node{%v} remove node{%v}", r.id, id)
	newPeers := make([]uint64, 0)
	for _, peer := range r.peers {
		if peer != id {
			newPeers = append(newPeers, peer)
		}
	}
	r.peers = newPeers
	delete(r.Prs, id)
	if r.State == StateLeader {
		if len(r.peers) != 0 {
			if r.maybeCommit() {
				r.broadcastAppendEntry()
			}
		}
	}
	if r.State == StateLeader && r.leadTransferee == id {
		r.abortLeaderTransfer()
	}
}
