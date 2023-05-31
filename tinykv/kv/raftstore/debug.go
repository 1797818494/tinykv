package raftstore

import "github.com/pingcap-incubator/tinykv/log"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Infof(format, a...)
	}
	return
}
