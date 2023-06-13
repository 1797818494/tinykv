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
	Match, Next       uint64
	LastCommunicateTs int
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
	Lead  uint64
	peers []uint64

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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	log.SetLevel(log.LOG_LEVEL_ERROR)
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
	raft.electionElapsed = 0
	raft.electionTimeout = c.ElectionTick
	raft.heartbeatElapsed = 0
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.ResetElectionTime()
	// var entrisFirst []*pb.Entry
	// entrisFirst = append(entrisFirst, &pb.Entry{})
	// raft.RaftLog.appendNewEntry(entrisFirst)
	for _, p := range raft.peers {
		raft.Prs[p] = &Progress{Match: 0, Next: 1}
	}
	// Your Code Here (2A).
	log.Infof("raft{%v} new succeed important state: vote{%v} term{%v} commit{%v} peers{%v}", raft.id, raft.Vote,
		raft.Term, raft.RaftLog.committed, raft.peers)
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
		// for _, entry := range ents {
		// 	log.Infof("Node{%v} sendAppend term{%v} index{%v} to{%v}", r.id, entry.Term, entry.Index, to)
		// }
		log.Debugf("Node{%v} sendAppend Next{%v} len(%v) to{%v}", r.id, prevLogIndex, len(r.RaftLog.entries), to)
		r.send(m)
		return true
	} else {
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
		log.Infof("Node{%v} sendsnap to{%v}", r.id, to)
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
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  commit,
	})
}

func (r *Raft) checkFollowerActive(to uint64) bool {
	internal := r.TickNum - r.Prs[to].LastCommunicateTs
	return internal < 15
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// r.mu.Lock()
	// defer r.mu.Unlock()
	r.TickNum++
	if r.TickNum >= r.electionElapsed+r.electionRadomTimeOut {
		if r.State == StateFollower || r.State == StateCandidate {
			// log.Infof("Node{%v} become candidate term{%v}, ticker{%v}", r.id, r.Term, r.TickNum)
			msg_hup := pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id, Term: r.Term}
			r.Step(msg_hup)
		}
	}
	// log.Infof("%v", r.heartbeatElapsed+r.heartbeatTimeout)
	if r.TickNum >= r.heartbeatElapsed+r.heartbeatTimeout {
		// log.Info(r.State.String())
		if r.State == StateLeader {
			log.Infof("Node{%v} start hearbeat term{%v}, ticker{%v}", r.id, r.Term, r.TickNum)
			msg_heart := pb.Message{MsgType: pb.MessageType_MsgBeat, To: r.id,
				From: r.id, Term: r.Term, Commit: r.RaftLog.committed}
			r.Step(msg_heart)
			// msg_append_no_op := pb.Message{MsgType: pb.MessageType_MsgAppend, To: r.id, From: r.id}
			// r.Step(msg_append_no_op)
		}
	}
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
	}
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		if m.MsgType == pb.MessageType_MsgHup {
			r.handleMsgUp(m)
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
	case StateCandidate:
		if m.MsgType == pb.MessageType_MsgHup {
			r.handleMsgUp(m)
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
	case StateLeader:
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
	}
	return nil
}
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	r.Prs[m.From].LastCommunicateTs = r.TickNum
	if m.Reject {
		// heartbeat 被拒绝的原因只可能是对方节点的 Term 更大
		r.becomeFollower(m.Term, None)
	} else {
		// 心跳同步成功

		// 检查该节点的日志是不是和自己是同步的，由于有些节点断开连接并又恢复了链接
		// 因此 leader 需要及时向这些节点同步日志
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
}

func (r *Raft) broadcastHeartBeat() {
	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
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
func (r *Raft) handleMsgUp(m pb.Message) {
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
	r.Prs[m.From].LastCommunicateTs = r.TickNum
	if m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		} else {
			r.Prs[m.From].Next = m.Index + 1
			r.sendAppend(m.From)
		}
		return
	}
	if r.Prs[m.From].maybeUpdate(m.Index) {
		if r.maybeCommit() {
			r.broadcastAppendEntry()
		}
	}
}
func (r *Raft) maybeCommit() bool {
	matchArray := make(uint64Slice, 0)
	for _, progress := range r.Prs {
		matchArray = append(matchArray, progress.Match)
	}
	// 获取所有节点 match 的中位数，就是被大多数节点复制的日志索引
	sort.Sort(sort.Reverse(matchArray))
	majority := len(r.Prs)/2 + 1
	toCommitIndex := matchArray[majority-1]
	// 检查是否可以提交 toCommitIndex
	return r.RaftLog.maybeCommit(toCommitIndex, r.Term)
}

func (r *Raft) broadcastAppendEntry() {
	for id := range r.Prs {
		if id == r.id {
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
			for _, ent := range r.RaftLog.entries {
				if ent.Term == conflictTerm {
					appendEntryResp.Index = ent.Index - 1
					break
				}
			}
		}
		log.Debugf("Node{%v} appendfail idx{%v}", r.id, appendEntryResp.Index)
	} else {
		if len(m.Entries) > 0 {
			idx, newLogIndex := m.Index+1, m.Index+1
			for ; idx < r.RaftLog.LastIndex() && idx <= m.Entries[len(m.Entries)-1].Index; idx++ {
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
				if idx < r.RaftLog.committed {
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
	if m.Term > r.Term {
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
	}
	if r.Term > m.Term {
		heartBeatResp.Reject = true
	} else {
		// if m.Commit > r.RaftLog.LastIndex() {
		// 	panic("heartbeat not match")
		// }
		// if m.Commit > r.RaftLog.committed {
		// 	r.RaftLog.commit(m.Commit)
		// }
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
	for _, peer := range r.peers {
		r.Prs[peer] = &Progress{}
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
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
