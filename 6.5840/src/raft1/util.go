package raft

import (
	"log"
	"math/rand"
)

// Debugging
const Debug = false

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func Random() int64 {
	return (50 + (rand.Int63() % 300))
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

const Leader uint8 = 0
const Candidate uint8 = 1
const Follower uint8 = 2
