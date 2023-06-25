package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn    *MvccTxn
	preKey []byte
	iter   engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfDefault)
	iter.Seek(EncodeKey(startKey, TsMax))

	return &Scanner{txn: txn, iter: iter, preKey: make([]byte, 0)}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.txn.Reader.Close()
}

type ErrEnd struct {
}

func (e ErrEnd) Error() string {
	return "reach end"
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for {
		if !scan.iter.Valid() {
			return nil, nil, ErrEnd{}
		}
		key := scan.iter.Item().KeyCopy(nil)
		// value, err := scan.iter.Item().ValueCopy(nil)
		// ts := decodeTimestamp(key)
		user_key := DecodeUserKey(key)
		scan.iter.Next()
		// the same key with preKey should continue
		if bytes.Equal(user_key, scan.preKey) {
			continue
		}
		iter := scan.txn.Reader.IterCF(engine_util.CfWrite)
		key_ts := EncodeKey(user_key, scan.txn.StartTS)
		iter.Seek(key_ts)
		for {
			if !iter.Valid() {
				break
			}
			key_raw := iter.Item().Key()
			ts := decodeTimestamp(key_raw)
			key_1 := DecodeUserKey(key_raw)
			if !bytes.Equal(user_key, key_1) {
				// no write on the key
				break
			}
			scan.preKey = user_key
			val, err := iter.Item().Value()
			if err != nil {
				panic("val err")
			}
			iter.Next()
			write, err_p := ParseWrite(val)
			if err_p != nil {
				panic("parse err")
			}

			if write.Kind == WriteKindPut && ts <= scan.txn.StartTS {
				// find the latest
				log.Infof("ts{%v} timestamp{%v}", ts, scan.txn.StartTS)
				scan.iter.Seek(EncodeKey(user_key, write.StartTS))
				value, err := scan.iter.Item().ValueCopy(nil)
				if err != nil {
					return nil, nil, err
				}
				return user_key, value, nil
			}
			if (write.Kind == WriteKindRollback || write.Kind == WriteKindDelete) && ts <= scan.txn.StartTS {
				// if deleted, impossible
				if write.Kind == WriteKindDelete {
					break
				}
				// if rollback, look back
				continue
			}

		}
	}

	return nil, nil, nil
}
