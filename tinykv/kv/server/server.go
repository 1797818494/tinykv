package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).

	return nil, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	var keys [][]byte = make([][]byte, 0)
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	resp := new(kvrpcpb.PrewriteResponse)
	txn := server.mvccGenerate(req.StartVersion, req.Context)
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	for _, key := range keys {
		if write, _, _ := txn.MostRecentWrite(key); write != nil {
			return resp, &mvcc.KeyError{KeyError: kvrpcpb.KeyError{Abort: "write conflict"}}
		}
	}
	if server.checkRowLocked(keys, txn) {
		return resp, &mvcc.KeyError{KeyError: kvrpcpb.KeyError{Abort: "lockfail"}}
	}
	for _, mutation := range req.Mutations {
		if mutation.Op == kvrpcpb.Op_Del {
			txn.DeleteValue(mutation.Key)
		}
		if mutation.Op == kvrpcpb.Op_Put {
			txn.PutValue(mutation.Key, mutation.Value)
		}
		// op_lock no used
	}
	return resp, nil
}

func (server *Server) mvccGenerate(startVersion uint64, ctx *kvrpcpb.Context) mvcc.MvccTxn {
	reader, err := server.storage.Reader(ctx)
	if err != nil {
		panic("reader err")
	}
	return mvcc.MvccTxn{StartTS: startVersion, Reader: reader}
}
func (server *Server) checkRowLocked(keys [][]byte, txn mvcc.MvccTxn) bool {
	for _, key := range keys {
		lock, _ := txn.GetLock(key)
		if lock != nil && lock.Ts != txn.StartTS {
			return true
		}
	}
	return false
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.CommitResponse)
	txn := server.mvccGenerate(req.StartVersion, req.Context)
	server.Latches.AcquireLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		if lock, _ := txn.GetLock(key); lock != nil {
			if lock.Ts != txn.StartTS {
				resp.Error.Abort = "abort"
				return resp, nil
			}
		}
	}
	for _, key := range req.Keys {
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindPut})
	}
	for _, key := range req.Keys {
		txn.DeleteLock(key)
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
