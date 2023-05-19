package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	// 存储包含自上次快照以来的所有稳定条目。
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// 已知已提交的最高的日志条目的索引     committedIndex
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 已经被应用到状态机的最高的日志条目的索引 appliedIndex
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// stabled 保存的是已经持久化到 storage 的 index
	stabled uint64

	// all entries that have not yet compact.
	// 尚未压缩的所有条目
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	//收到 leader 的快照的时候，会将快照保存在此处，后续会把快照保存到 Ready 中去
	//上层应用会应用 Ready 里面的快照
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	//用于节点记录上一条追加的日志 Index
	//在 follower 更新自己的 committed 时
	//需要把leader 传来的 committed 和其进行比较
	dummyIndex uint64
	Vote       uint64
	curTerm    uint64
}

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		panic("err")
	}
	l.applied = i
}

func (l *RaftLog) stableTo(i uint64) {
	l.stabled = i
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
// newLog使用给定存储返回日志。它将日志恢复到刚刚提交并应用最新快照的状态。
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	hardState, _, _ := storage.InitialState()

	rl := &RaftLog{
		storage:    storage,
		committed:  hardState.Commit,
		applied:    firstIndex - 1,
		stabled:    lastIndex,
		entries:    entries,
		dummyIndex: firstIndex,
		curTerm:    hardState.Term,
		Vote:       hardState.Vote,
	}

	return rl
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
//我们需要在某个时间点压缩日志条目，例如
//存储压缩稳定日志条目阻止日志条目在内存中无限增长
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	newFirst, _ := l.storage.FirstIndex()
	if newFirst > l.dummyIndex {
		//为了GC原来的 所以append
		entries := l.entries[newFirst-l.dummyIndex:]
		l.entries = make([]pb.Entry, 0)
		l.entries = append(l.entries, entries...)
	}
	l.dummyIndex = newFirst
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
// allEntries返回所有未压缩的条目。
// 注意，从返回值中排除任何虚拟条目。
// 注意，这是您需要实现的测试存根函数之一。
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
// 返回所有未持久化到 storage 的日志
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.LastIndex()-l.stabled == 0 {
		return make([]pb.Entry, 0)
	}
	return l.getEntries(l.stabled+1, 0)
}

// getEntries 返回 [start, end) 之间的所有日志，end = 0 表示返回 start 开始的所有日志
func (l *RaftLog) getEntries(start uint64, end uint64) []pb.Entry {
	if end == 0 {
		end = l.LastIndex() + 1
	}
	start, end = start-l.dummyIndex, end-l.dummyIndex
	return l.entries[start:end]
}

// nextEnts returns all the committed but not applied entries
// 返回所有已经提交但没有应用的日志
// 返回在(applied，committed]之间的日志
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	//fst applied=5 , committed=5 , dummyIndex=6
	//sec applied=5 , committed=10 , dummyIndex=6
	//want [6,7,8,9,10]
	//idx  [0,1,2,3,4 , 5) ===>[0,5)
	//diff = dummyIndex - 1 =5

	diff := l.dummyIndex - 1
	if l.committed > l.applied {
		return l.entries[l.applied-diff : l.committed-diff]
	}
	return make([]pb.Entry, 0)
}

// LastIndex return the last index of the log entries
// 返回日志项的最后一个索引
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.dummyIndex - 1 + uint64(len(l.entries))
}

// Term return the term of the entry in the given index
// 返回给定索引中log的term
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i >= l.dummyIndex {
		return l.entries[i-l.dummyIndex].Term, nil
	}
	// 2. 判断 i 是否等于当前正准备安装的快照的最后一条日志
	if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	// 3. 否则的话 i 只能是快照中的日志
	term, err := l.storage.Term(i)
	return term, err
}

// LastTerm 返回最后一条日志的索引
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	lastIndex := l.LastIndex() - l.dummyIndex
	return l.entries[lastIndex].Term
}

// 选举限制
func (l *RaftLog) isUpToDate(index, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && index >= l.LastIndex())
}

func (l *RaftLog) TermNoErr(i uint64) uint64 {
	//1.
	if i >= l.dummyIndex {
		return l.entries[i-l.dummyIndex].Term
	}
	//2.
	if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term
	}
	//3.debug here
	term, _ := l.storage.Term(i)
	return term
}

func (l *RaftLog) truncate(startIndex uint64) {
	if len(l.entries) > 0 {
		l.entries = l.entries[:startIndex-l.dummyIndex]
	}
}

// appendEntry 添加新的日志，并返回最后一条日志的索引
func (l *RaftLog) appendNewEntry(ents []*pb.Entry) uint64 {
	for i := range ents {
		l.entries = append(l.entries, *ents[i])
	}
	return l.LastIndex()
}

func (l *RaftLog) commit(toCommit uint64) {
	l.committed = toCommit
}

// maybeCommit 检查一个被大多数节点复制的日志是否需要提交
func (l *RaftLog) maybeCommit(toCommit, term uint64) bool {
	commitTerm, _ := l.Term(toCommit)
	if toCommit > l.committed && commitTerm == term {
		// 只有当该日志被大多数节点复制（函数调用保证），并且日志索引大于当前的commitIndex（Condition 1）
		// 并且该日志是当前任期内创建的日志（Condition 2），才可以提交这条日志
		// 【注】为了一致性，Raft 永远不会通过计算副本的方式提交之前任期的日志，只能通过提交当前任期的日志一并提交之前所有的日志
		l.commit(toCommit)
		return true
	}
	return false
}
