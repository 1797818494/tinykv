package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	responce := &kvrpcpb.RawGetResponse{}
	reader, err1 := server.storage.Reader(req.GetContext())
	if err1 != nil {
		responce.NotFound = true
		return responce, err1
	}
	cf_value, err2 := reader.GetCF(req.Cf, req.Key)
	if err2 != nil {
		responce.NotFound = true
		return responce, err1
	}
	if cf_value == nil {
		responce.NotFound = true
	}
	responce.Value = cf_value
	// iter := reader.IterCF(req.Cf)
	// iter.Seek(cf_key)
	// if iter.Valid() {
	// 	log.Debugf("get key{%v} cf_key{%v} valid", iter.Item().Key(), cf_key)
	// 	if bytes.Equal(iter.Item().Key(), cf_key) {
	// 		var err3 error
	// 		responce.Value, err3 = iter.Item().Value()
	// 		log.Debugf("value{%v}", responce.Value)
	// 		if err3 != nil {
	// 			return nil, err3
	// 		}
	// 	} else {
	// 		responce.NotFound = true
	// 	}

	// } else {
	// 	responce.NotFound = true
	// }

	return responce, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	responce := &kvrpcpb.RawPutResponse{}
	var batch []storage.Modify
	batch = append(batch, storage.Modify{storage.Put{Cf: req.Cf, Key: req.Key, Value: req.Value}})
	err := server.storage.Write(req.Context, batch)
	return responce, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	responce := &kvrpcpb.RawDeleteResponse{}
	var batch []storage.Modify
	batch = append(batch, storage.Modify{storage.Delete{Cf: req.Cf, Key: req.Key}})
	err := server.storage.Write(req.Context, batch)
	return responce, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	responce := &kvrpcpb.RawScanResponse{}
	reader, err1 := server.storage.Reader(req.GetContext())
	if err1 != nil {
		return responce, err1
	}
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	var nums uint32 = 0
	for ; iter.Valid(); iter.Next() {
		if nums == req.GetLimit() {
			break
		}
		value, err := iter.Item().Value()
		if err != nil {
			return responce, err
		}
		responce.Kvs = append(responce.Kvs, &kvrpcpb.KvPair{Key: iter.Item().Key(), Value: value})
		nums++
	}
	return responce, nil
}
