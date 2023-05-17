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
	"sync"
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
	Match, Next uint64
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
	mu    sync.Mutex
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
	voteGrandNum   int
	voteFailNum    int

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
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	log.SetLevel(log.LOG_LEVEL_DEBUG)
	var raft Raft
	raft.id = c.ID
	raft.RaftLog = newLog(c.Storage)
	raft.Term = raft.RaftLog.term
	raft.Vote = raft.RaftLog.vote
	raft.Prs = make(map[uint64]*Progress)
	raft.votes = make(map[uint64]bool)
	raft.peers = c.peers
	raft.TickNum = 0
	raft.electionElapsed = 0
	raft.electionTimeout = c.ElectionTick
	raft.heartbeatElapsed = 0
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.ResetElectionTime()
	// Your Code Here (2A).
	return &raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// r.mu.Lock()
	// defer r.mu.Unlock()
	r.TickNum++
	// log.Infof("Node{%v} tick{%v}", r.id, r.TickNum)
	if r.TickNum >= r.electionElapsed+r.electionRadomTimeOut {
		if r.State == StateFollower || r.State == StateCandidate {
			// log.Infof("Node{%v} become candidate term{%v}, ticker{%v}", r.id, r.Term, r.TickNum)
			msg_hup := pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id, Term: r.Term}
			r.Step(msg_hup)
		}
	}
	// log.Infof("%v", r.heartbeatElapsed+r.heartbeatTimeout)
	if r.TickNum >= r.heartbeatElapsed+r.heartbeatTimeout {
		if r.State == StateLeader {
			r.becomeLeader()
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
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	log.Infof("Node{%v} become follower term{%v}, ticker{%v}", r.id, r.Term, r.TickNum)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.State = StateCandidate
	r.voteGrandNum = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Infof("Node{%v} become leader term{%v}, ticker{%v}", r.id, r.Term, r.TickNum)
	r.voteGrandNum = 0
	r.State = StateLeader
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		if m.MsgType == pb.MessageType_MsgHup {
			r.becomeCandidate()
			r.ResetElectionTime()
			r.Step(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: r.id, Reject: false, Term: r.Term})
			for _, peer := range r.peers {
				if peer != r.id {
					request_vote_msg := pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: peer, Term: r.Term, LogTerm: r.RaftLog.GetLastTerm(),
						Index: r.RaftLog.GetLastTerm()}
					r.msgs = append(r.msgs, request_vote_msg)
				}
			}
		}
	case StateCandidate:
		if m.MsgType == pb.MessageType_MsgHup {
			r.becomeCandidate()
			r.ResetElectionTime()
			r.Step(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: r.id, Reject: false, Term: r.Term})
			for _, peer := range r.peers {
				if peer != r.id {
					request_vote_msg := pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: peer, Term: r.Term, LogTerm: r.RaftLog.GetLastTerm(),
						Index: r.RaftLog.GetLastTerm()}
					r.msgs = append(r.msgs, request_vote_msg)
				}
			}
		}
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			log.Infof("Node{%v} from Node{%v} reject{%v}", m.To, m.From, m.Reject)
			// log.Infof("node{%v} receive from Node{%v}", m.To, m.From)
			if !m.Reject {
				r.voteGrandNum++
				r.votes[m.From] = true
				if r.voteGrandNum > len(r.peers)/2 {
					r.becomeLeader()
				}
			} else if m.Term > r.Term {
				r.Vote = 0
				r.becomeFollower(m.Term, m.From)
			}
			// if r.voteFailNum > len(r.peers)/2 {
			// 	r.becomeFollower(r.Term, 0)
			// }
		}
	case StateLeader:
		if m.MsgType == pb.MessageType_MsgBeat {
			for _, peer := range r.peers {
				if peer != r.id {
					heart_msg := pb.Message{MsgType: pb.MessageType_MsgHeartbeat, From: m.From, To: peer, Term: m.Term, Commit: m.Commit}
					r.msgs = append(r.msgs, heart_msg)
				}
			}
		}
		if m.MsgType == pb.MessageType_MsgPropose {
			r.RaftLog.AppendLogs(m.Entries)
		}
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
	if m.MsgType == pb.MessageType_MsgHeartbeat {
		r.handleHeartbeat(m)
	}
	return nil
}
func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject && m.Term > r.Term {
		r.becomeFollower(m.Term, 0)
		r.Vote = 0
		return
	}
	if m.Reject && m.Term < r.Term {
		log.Fatalf("responce term small")
	}
	if m.Reject && m.Term == r.Term {
		r.Prs[m.From].Next = m.Index - 1
		if r.Prs[m.From].Next < r.Prs[m.From].Match+1 {
			r.Prs[m.From].Next = r.Prs[m.From].Match + 1
		}
		return
	}
	if !m.Reject {
		r.Prs[m.From].Next = m.Index
		r.Prs[m.From].Match = max(r.Prs[m.From].Next-1, r.Prs[m.From].Match)
		matchIdx := make([]uint64, 0)
		for _, state := range r.Prs {
			matchIdx = append(matchIdx, state.Match)
		}
		matchIdx = append(matchIdx, r.RaftLog.LastIndex())
		sort.Slice(matchIdx, func(i, j int) bool {
			return matchIdx[i] < matchIdx[j]
		})
		commitIdx := matchIdx[len(r.peers)/2]
		if commitIdx > r.RaftLog.committed && r.RaftLog.GetLastTerm() == r.Term {
			r.RaftLog.committed = commitIdx
			// apply to do
		}
	} else {
		r.becomeFollower(m.Term, 0)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	responce := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: m.To, To: m.From}
	if m.Term < r.Term {
		responce.Term = r.Term
		responce.Reject = true
		return
	}
	if m.Term > r.Term {
		r.Term, r.Vote = m.Term, m.From
	}
	r.becomeFollower(r.Term, m.From)
	r.ResetElectionTime()
	// if !r.checkLog(m.Entries, m.Index) {
	// 	r.RaftLog.CutAndAppendLogs(m.Index, m.Entries)
	// }
	r.Term = m.Term
	if r.RaftLog.committed < m.Commit {
		r.RaftLog.committed = max(m.Commit, r.RaftLog.LastIndex())
	}
	responce.Term = r.Term
	responce.Reject = false

}

func (r *Raft) checkLog(args []*pb.Entry, preIdx uint64) bool {
	log := r.RaftLog.entries
	log = log[preIdx+1:]
	if len(log) == 0 {
		return false
	}
	flag := true
	if len(log) > len(args) {
		for i := 0; i < len(args); i++ {
			if log[i].Index != args[i].Index || log[i].Term != args[i].Term {
				flag = false
				break
			}
		}
	} else {
		flag = false
	}
	return flag
}

func (r *Raft) handleRequestVote(m pb.Message) {
	//2A
	log.Debugf("Node{%v} process requestvote from Node{%v}", m.To, m.From)
	responce := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse,
		To: m.From, From: m.To, Reject: false, Term: m.Term}
	defer func() {
		log.Debug("Node{%v} from Node{%v} votereply Term{%v} reject{%v}",
			m.To, m.From, responce.Term, responce.Reject)
		r.msgs = append(r.msgs, responce)
	}()
	if m.Term < r.Term || (m.Term == r.Term && r.Vote != 0 && r.Vote != m.From) {
		responce.Term = r.Term
		responce.Reject = true
		log.Debugf("Node{%v} reject_vote1 Node{%v}", m.To, m.From)
		return
	}
	if m.Term > r.Term {
		log.Debugf("Node{%v} requesthigher term from{%v} to {%v} ", r.id, r.Term, m.Term)
		r.becomeFollower(r.Term, m.From)
		r.Vote = 0
	}
	if m.LogTerm < r.RaftLog.GetLastTerm() {
		responce.Term = r.Term
		responce.Reject = true
		log.Debugf("Node{%v} reject_vote2 Node{%v}", m.To, m.From)
		return
	}
	if m.LogTerm == r.RaftLog.GetLastTerm() && m.Index < r.RaftLog.LastIndex() {
		responce.Term = r.Term
		responce.Reject = true
		log.Debugf("Node{%v} reject_vote3 Node{%v}", m.To, m.From)
		return
	}
	r.Term = m.Term
	r.Vote = m.From
	responce.Term = r.Term

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
	responce := pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse,
		To: m.From, From: m.To, Reject: false, Term: m.Term}
	defer func() {
		r.msgs = append(r.msgs, responce)
	}()
	log.Debugf("Node{%v} from Node{%v} heart beat", m.To, m.From)
	if m.Term < r.Term {
		responce.Term = r.Term
		responce.Reject = true
		return
	}
	if m.Term > r.Term {
		r.Term, r.Vote = m.Term, 0
	}
	r.becomeFollower(r.Term, m.From)
	r.ResetElectionTime()
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
