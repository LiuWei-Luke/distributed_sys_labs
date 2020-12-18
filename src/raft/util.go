package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		a = append(a, time.Now().UnixNano()/1e6)
		log.Printf(format + " ---- %v", a...)
	}
	return
}
