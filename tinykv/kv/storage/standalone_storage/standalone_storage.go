package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db     *badger.DB
	dbpath string
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{dbpath: conf.DBPath}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.dbpath
	opts.ValueDir = s.dbpath
	db, err := badger.Open(opts)
	s.db = db
	return err
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	reader := AloneReader{txn: txn}
	return &reader, nil
}

type AloneReader struct {
	txn *badger.Txn
}

func (reader *AloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err != nil {
		return nil, nil
	}
	return val, err
}
func (reader *AloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}
func (reader *AloneReader) Close() {
	reader.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	for _, m := range batch {
		var err error = nil
		switch m.Data.(type) {
		case storage.Delete:
			err = txn.Delete(engine_util.KeyWithCF(m.Cf(), m.Key()))
		case storage.Put:
			err = txn.Set(engine_util.KeyWithCF(m.Cf(), m.Key()), m.Value())
		}
		if err != nil {
			return err
		}
	}
	err := txn.Commit()
	log.Debugf("success commit txn batch[%v]", batch)
	return err
}
