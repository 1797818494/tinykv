package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
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
	resp := new(kvrpcpb.GetResponse)
	txn, err := server.mvccGenerate(req.Version, req.Context)
	if err != nil {
		return resp, err
	}
	keys := make([][]byte, 0)
	keys = append(keys, req.Key)
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	lock, _ := txn.GetLock(req.Key)

	if lock != nil && lock.Ts < req.Version {
		log.Infof("lock ts{%v} startTs{%v}", lock.Ts, req.Version)
		resp.Error = new(kvrpcpb.KeyError)
		resp.Error.Locked = lock.Info(req.Key)
		return resp, nil
	}
	val, _ := txn.GetValue(req.Key)
	if val == nil {
		resp.NotFound = true
	} else {
		resp.Value = val
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	var keys [][]byte = make([][]byte, 0)
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	resp := new(kvrpcpb.PrewriteResponse)
	txn, err := server.mvccGenerate(req.StartVersion, req.Context)
	if err != nil {
		return resp, err
	}
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	log.Infof("startverison{%v}", req.StartVersion)
	for _, key := range keys {
		if write, ts, _ := txn.MostRecentWrite(key); write != nil && ts >= req.StartVersion {
			log.Infof("confilct")
			resp.Errors = make([]*kvrpcpb.KeyError, 0)
			key_err := new(kvrpcpb.KeyError)
			key_err.Conflict = &kvrpcpb.WriteConflict{StartTs: req.StartVersion, ConflictTs: ts, Key: key, Primary: req.PrimaryLock}
			resp.Errors = append(resp.Errors, key_err)
		}
	}
	if resp.Errors != nil {
		return resp, nil
	}
	for _, key := range keys {
		lock, _ := txn.GetLock(key)
		if lock != nil {
			log.Infof("confilct")
			resp.Errors = make([]*kvrpcpb.KeyError, 0)
			key_err := new(kvrpcpb.KeyError)
			key_err.Locked = lock.Info(key)
			resp.Errors = append(resp.Errors, key_err)
		}
	}
	if resp.Errors != nil {
		return resp, nil
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
	for _, mutation := range req.Mutations {
		if mutation.Op == kvrpcpb.Op_Del {
			log.Infof("1")
			txn.PutLock(mutation.Key, &mvcc.Lock{Primary: req.PrimaryLock, Ts: req.StartVersion, Ttl: req.LockTtl, Kind: mvcc.WriteKindDelete})
		}
		if mutation.Op == kvrpcpb.Op_Put {
			log.Infof("2")
			txn.PutLock(mutation.Key, &mvcc.Lock{Primary: req.PrimaryLock, Ts: req.StartVersion, Ttl: req.LockTtl, Kind: mvcc.WriteKindPut})
		}
	}
	server.storage.Write(req.Context, txn.Writes())
	return resp, nil
}

func (server *Server) mvccGenerate(startVersion uint64, ctx *kvrpcpb.Context) (mvcc.MvccTxn, error) {
	reader, err := server.storage.Reader(ctx)
	// when region not match, the err should return
	if err != nil {
		return mvcc.MvccTxn{}, err
	}
	return mvcc.MvccTxn{StartTS: startVersion, Reader: reader}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.CommitResponse)
	txn, err := server.mvccGenerate(req.StartVersion, req.Context)
	if err != nil {
		return resp, err
	}
	server.Latches.AcquireLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	keys_should_commit := make([][]byte, 0)
	log.Infof("commitTs{%v} startTs{%v}", req.CommitVersion, req.StartVersion)
	for _, key := range req.Keys {
		lock, _ := txn.GetLock(key)
		if lock != nil && lock.Ts != txn.StartTS {
			resp.Error = new(kvrpcpb.KeyError)
			resp.Error.Retryable = "retry"
			return resp, nil
		}
		if lock == nil {
			write, ts, _ := txn.CurrentWrite(key)
			if write != nil {
				if ts == req.CommitVersion {
					if write.Kind == mvcc.WriteKindRollback {
						resp.Error = new(kvrpcpb.KeyError)
						resp.Error.Abort = "abort"
						return resp, nil
					}
				}
			}
		} else {
			keys_should_commit = append(keys_should_commit, key)
		}

	}

	for _, key := range keys_should_commit {
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindPut})
	}

	for _, key := range keys_should_commit {
		txn.DeleteLock(key)
	}
	server.storage.Write(req.Context, txn.Writes())
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	txn, err := server.mvccGenerate(req.Version, req.Context)
	if err != nil {
		return nil, err
	}
	scanner := mvcc.NewScanner(req.StartKey, &txn)
	resp := new(kvrpcpb.ScanResponse)
	resp.Pairs = make([]*kvrpcpb.KvPair, 0)
	// defer scanner.Close()
	var cnt uint32 = 0
	for {
		key, value, err := scanner.Next()
		err_end := mvcc.ErrEnd{}
		if err == err_end {
			break
		}
		if cnt == req.Limit {
			return resp, nil
		}
		if err != nil {
			cnt++
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Error: &kvrpcpb.KeyError{Retryable: "retry"}})
		}
		if err == nil {
			cnt++
			log.Infof("get key{%v} value{%v}", key, value)
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Value: value})
		}
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	txn, err := server.mvccGenerate(req.LockTs, req.Context)
	if err != nil {
		return nil, err
	}
	resp := new(kvrpcpb.CheckTxnStatusResponse)
	resp.Action = kvrpcpb.Action_NoAction
	lock, _ := txn.GetLock(req.PrimaryKey)
	// timeout
	if lock != nil && lock.Ts == req.LockTs {
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			log.Info("stale")
			txn.DeleteValue(req.PrimaryKey)
			txn.DeleteLock(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback})
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			server.storage.Write(req.Context, txn.Writes())
			return resp, nil
		}
	}
	// lock
	if lock != nil {
		resp.LockTtl = lock.Ttl
		return resp, nil
	}
	write, version, _ := txn.CurrentWrite(req.PrimaryKey)
	// abort
	if write == nil {
		resp.CommitVersion = 0
		resp.LockTtl = 0
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback})
		server.storage.Write(req.Context, txn.Writes())
		return resp, nil
	}
	// commit
	resp.CommitVersion = version
	resp.LockTtl = 0
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.BatchRollbackResponse)
	txn, err := server.mvccGenerate(req.StartVersion, req.Context)
	if err != nil {
		return resp, nil
	}
	write := new(mvcc.Write)
	write.StartTS = req.StartVersion
	write.Kind = mvcc.WriteKindRollback
	// write := &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback}
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			panic(err)
		}
		if lock != nil && lock.Ts == req.StartVersion {
			txn.DeleteValue(key)
			txn.DeleteLock(key)
			txn.PutWrite(key, req.StartVersion, write)
			log.Infof("putwrite")
		} else {
			write, _, _ := txn.CurrentWrite(key)
			if write == nil {
				txn.PutWrite(key, req.StartVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
			}
			if write != nil {
				if write.Kind != mvcc.WriteKindRollback {
					resp.Error = new(kvrpcpb.KeyError)
					resp.Error.Abort = "abort"
					return resp, nil
				}
			}
		}
	}
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		panic(err)
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ResolveLockResponse)
	txn, err := server.mvccGenerate(req.StartVersion, req.Context)
	if err != nil {
		return resp, nil
	}
	iter := txn.Reader.IterCF(engine_util.CfLock)
	for {
		if !iter.Valid() {
			break
		}
		key := iter.Item().Key()
		value, err := iter.Item().Value()
		iter.Next()
		if err != nil {
			panic(err)
		}
		lock, err_lock := mvcc.ParseLock(value)
		if err_lock != nil {
			panic(err_lock)
		}
		write, _, _ := txn.CurrentWrite(key)
		if lock != nil && lock.Ts != txn.StartTS {
			// other transaction locked the key
			continue
		}
		if write == nil {
			if req.StartVersion < req.CommitVersion {
				// commit
				txn.DeleteLock(key)
				txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindPut})
			}
			if req.StartVersion > req.CommitVersion {
				// rollback
				txn.DeleteValue(key)
				txn.DeleteLock(key)
				txn.PutWrite(key, req.StartVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
			}
		}
	}
	server.storage.Write(req.Context, txn.Writes())

	return resp, nil
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
