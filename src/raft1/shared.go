package raft

import (
	"math/rand"
	"time"

	"6.5840/raftapi"
)

// needs to be called with rf.mu locked
func (rf *Raft) isElectionTimedout() bool {
	// the randomness added for election time checking
	r := electionTimeout + (rand.Int63() % electionJitter)
	t := time.Duration(r) * time.Millisecond
	return rf.lastBeat.Before(time.Now().Add(-t))
}

// needs to be called with rf.mu locked
func (rf *Raft) increaseTerm(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1

	rf.persist()
}

// needs to be called with rf.mu locked
func (rf *Raft) castVote(peer int) {
	rf.votedFor = peer

	rf.persist()
}

// needs to be called with rf.mu locked
func (rf *Raft) resetVotedFor() {
	rf.castVote(-1)
}

// needs to be called with rf.mu locked
func (rf *Raft) needsElection() bool {
	return rf.votedFor == -1 || rf.isElectionTimedout()
}

// needs to be called with rf.mu locked
func (rf *Raft) getLastLogEntry() LogEntry {
	// li := rf.logIndexes[len(rf.logIndexes)-1]
	li := len(rf.logs) - 1
	return rf.logs[li]
}

// needs to be called with rf.mu locked
func (rf *Raft) sendCommittedToApp() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: int(rf.lastApplied),
		}
		rf.logger.Info("sent committed to app",
			"currentTerm", rf.currentTerm,
			"commitIndex", rf.commitIndex,
			"lastApplied", rf.lastApplied,
			"entry", rf.logs[rf.lastApplied],
		)
	}
}

// needs to be called with rf.mu locked
func (rf *Raft) addEntryToLog(e LogEntry) {
	rf.logs[e.Id] = e
	rf.logIndexes = append(rf.logIndexes, e.Id)

	rf.persist()
}
