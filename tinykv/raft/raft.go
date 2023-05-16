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
	"sync"

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
	heartbeatElapsed int
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
	r.mu.Lock()
	defer r.mu.Unlock()
	r.TickNum++
	if r.TickNum > r.electionElapsed+r.electionTimeout {
		if r.State == StateFollower || r.State == StateCandidate {
			r.becomeCandidate()
		}
	}
	if r.TickNum > r.heartbeatElapsed+r.heartbeatTimeout {
		if r.State == StateLeader {
			r.becomeLeader()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.mu.Lock()
	defer r.mu.Unlock()
	r.voteGrandNum = 0
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Term++
	r.voteGrandNum++
	msg_hup := pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id}
	r.msgs = append(r.msgs, msg_hup)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.mu.Lock()
	defer r.mu.Unlock()
	r.voteGrandNum = 0
	r.State = StateLeader
	msg_heart := pb.Message{MsgType: pb.MessageType_MsgBeat, To: r.id,
		From: r.id, Commit: r.RaftLog.committed}
	msg_append_no_op := pb.Message{MsgType: pb.MessageType_MsgAppend, To: r.id, From: r.id}
	r.msgs = append(r.msgs, msg_heart)
	r.msgs = append(r.msgs, msg_append_no_op)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		if m.MsgType == pb.MessageType_MsgHup {
			r.State = StateCandidate
			for _, peer := range r.peers {
				if peer != r.id {
					request_vote_msg := pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: peer, Term: r.Term, LogTerm: r.RaftLog.entries[len(r.RaftLog.entries)-1].Term,
						Index: r.RaftLog.entries[len(r.RaftLog.entries)-1].Index}
					r.msgs = append(r.msgs, request_vote_msg)
				}
			}
		}
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
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
	case StateCandidate:
		if m.MsgType == pb.MessageType_MsgHup {
			r.State = StateCandidate
			for _, peer := range r.peers {
				if peer != r.id {
					request_vote_msg := pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: peer, Term: r.Term, LogTerm: r.RaftLog.entries[len(r.RaftLog.entries)-1].Term,
						Index: r.RaftLog.entries[len(r.RaftLog.entries)-1].Index}
					r.msgs = append(r.msgs, request_vote_msg)
				}
			}
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
	}
	if m.MsgType == pb.MessageType_MsgRequestVote {
		r.handleRequestVote(m)
	}
	if m.MsgType == pb.MessageType_MsgHeartbeat {
		responce := pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse,
			To: m.From, From: m.To, Reject: false, Term: m.Term}
		defer func() {
			r.msgs = append(r.msgs, responce)
		}()
		log.Debugf("Node{%v} from Node{%v} heart beat", m.To, m.From)
		if m.Term < r.Term {
			responce.Term = r.Term
			responce.Reject = true
			return nil
		}
		if m.Term > r.Term {
			r.Term, r.Vote = m.Term, 0
		}
		r.becomeFollower(r.Term, m.From)
		r.ResetElectionTime()

		return nil
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
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
		return
	}
	if m.Term > r.Term {
		log.Debugf("Node{%v} requesthigher term from{%v} to {%v} ", r.id, r.Term, m.Term)
		r.becomeFollower(r.Term, m.From)
		r.Vote = 0
	}
	if m.LogTerm < r.RaftLog.lastLog().Term {
		responce.Term = r.Term
		responce.Reject = true
		return
	}
	if m.LogTerm == r.RaftLog.lastLog().Term && m.Index < r.RaftLog.lastLog().Index {
		responce.Term = r.Term
		responce.Reject = true
		return
	}
	r.Vote = m.From
	responce.Term = r.Term

}

func (r *Raft) ResetElectionTime() {
	r.electionElapsed = r.TickNum
}

func (r *Raft) ResetHeartTime() {
	r.heartbeatElapsed = r.TickNum
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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
