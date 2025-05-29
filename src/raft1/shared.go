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
	li := len(rf.logs) - 1
	if li < 0 {
		return LogEntry{Id: rf.snapshotLastIndex, Term: rf.snapshotlastTerm}
	}
	return rf.logs[li]
}

// needs to be called with rf.mu locked
func (rf *Raft) getLogEntry(id int) LogEntry {
	return rf.logs[id-rf.snapshotLastIndex]
}

// needs to be called with rf.mu locked
func (rf *Raft) getLogLen() int {
	return rf.snapshotLastIndex + len(rf.logs)
}

// needs to be called with rf.mu locked
func (rf *Raft) sendCommittedToApp() {
	for rf.commitIndex > rf.lastApplied {
		if rf.killed() {
			break
		}
		rf.lastApplied++
		rf.sendLogCh <- rf.getLogEntry(rf.lastApplied)
	}
}

func (rf *Raft) apply() {
	for l := range rf.sendLogCh {
		if rf.killed() {
			continue
		}
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      l.Command,
			CommandIndex: l.Id,
		}
		rf.logger.Info("sent committed to app",
			"currentTerm", rf.currentTerm,
			"commitIndex", rf.commitIndex,
			"lastApplied", rf.lastApplied,
			"entry", l,
		)
	}
}

// needs to be called with rf.mu locked
func (rf *Raft) addEntryToLog(e LogEntry) {
	rf.logs = append(rf.logs, e)
	rf.persist()
}

// needs to be called with rf.mu locked
func (rf *Raft) findFirstIndexForTerm(t int) int {
	l := -1
	for i := 0; i < len(rf.logs); i++ {
		if rf.logs[i].Term == t {
			l = i
			break
		}
	}
	return l
}

// needs to be called with rf.mu locked
func (rf *Raft) findLastIndexForTerm(t int) int {
	l := -1
	for i := len(rf.logs) - 1; i >= 0; i-- {
		if rf.logs[i].Term == t {
			l = i
			break
		}
	}
	return l
}
