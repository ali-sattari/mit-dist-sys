package raft

import (
	"fmt"
	"sort"
	"time"
)

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// needs to be called with rf.mu locked
func (rf *Raft) sendCommand(cmd any) int {
	l := LogEntry{
		Id:      rf.getLastLogEntry().Id + 1,
		Term:    rf.currentTerm,
		Command: cmd,
	}
	rf.addEntryToLog(l)

	rf.logger.Info("received a command",
		"currentTerm", rf.currentTerm,
		"entry", l,
	)

	return l.Id
}

// needs to be called with rf.mu locked
func (rf *Raft) replicateLogEntries(force bool) {
	// check if it is time to send heartbeat or entries
	if !force && time.Since(rf.lastBeat) < heartbeatInterval {
		return
	}

	rf.lastBeat = time.Now()

	for i := range rf.peers {
		// skip the leader itself
		if i == rf.me {
			continue
		}

		if rf.needsSnapshot(i) {
			rf.sendSnapshotToFollower(i)
			continue
		}

		entries := rf.getEntriesForFollower(i)

		// if not heartbeat
		if len(entries) > 0 {
			rf.logger.Debug(fmt.Sprintf("sending entry to %d", i),
				"currentTerm", rf.currentTerm,
				"nextIndex", rf.nextIndex[i],
				"matchIndex", rf.matchIndex[i],
				"snapshotLastIndex", rf.snapshotLastIndex,
				"len", len(entries),
			)
		} else {
			rf.logger.Debug(fmt.Sprintf("sending heartbeat to %d", i),
				"currentTerm", rf.currentTerm,
				"nextIndex", rf.nextIndex[i],
				"matchIndex", rf.matchIndex[i],
				"snapshotLastIndex", rf.snapshotLastIndex,
			)
		}

		rf.replicateEnteriesToFollower(entries, i)
	}
}

// needs to be called with rf.mu locked
func (rf *Raft) needsSnapshot(server int) bool {
	return rf.snapshotLastIndex > rf.nextIndex[server]
}

// needs to be called with rf.mu locked
func (rf *Raft) getEntriesForFollower(server int) []LogEntry {
	idx := rf.raftIdToSliceIndex(rf.nextIndex[server])
	if idx < 0 || idx >= len(rf.logs) {
		return []LogEntry{}
	}
	return rf.logs[idx:]
}

// needs to be called with rf.mu locked
func (rf *Raft) setFollowerIndexes() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.getLogLen() + 1
		rf.matchIndex[i] = 0
	}
}

// needs to be called with rf.mu locked
func (rf *Raft) replicateEnteriesToFollower(entries []LogEntry, follower int) {
	prevLogIndex := max(0, rf.nextIndex[follower]-1)
	a := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.getLogEntry(prevLogIndex).Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	r := AppendEntryReply{}

	go func() {
		if ok := rf.sendAppendEntry(follower, &a, &r); ok {
			rf.appendReplyCh <- AppendEntryResult{
				PeerId:           follower,
				Entries:          entries,
				AppendEntryReply: r,
			}
		}
	}()
}

// needs to be called with rf.mu locked
func (rf *Raft) sendSnapshotToFollower(server int) {
	a := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotLastIndex,
		LastIncludedTerm:  rf.snapshotlastTerm,
		Data:              rf.snapshot,
	}
	r := InstallSnapshotReply{}

	if ok := rf.sendInstallSnapshot(server, &a, &r); ok {
		if r.Term > rf.currentTerm {
			rf.increaseTerm(r.Term)
			rf.transition(Follower)
			return
		}
		rf.matchIndex[server] = rf.snapshotLastIndex
		rf.nextIndex[server] = rf.snapshotLastIndex + 1
	}
}

func (rf *Raft) receiveAppendReply() {
	for r := range rf.appendReplyCh {
		rf.mu.Lock()

		if rf.nodeRole != Leader {
			rf.logger.Debug("got append reply, not leader anymore",
				"currentTerm", rf.currentTerm,
				"commitIndex", rf.commitIndex,
				"reply", r,
			)
			rf.mu.Unlock()
			return
		}

		rf.logger.Debug("got append entry reply",
			"currentTerm", rf.currentTerm,
			"commitIndex", rf.commitIndex,
			"reply", r,
		)

		// step down if we get a higher term
		if rf.currentTerm < r.Term {
			rf.logger.Warn("stepping down due to higher term",
				"currentTerm", rf.currentTerm,
				"newTerm", r.Term)
			rf.increaseTerm(r.Term)
			rf.transition(Follower)
			rf.mu.Unlock()
			return
		}

		// reject response from an older term
		if rf.currentTerm != r.Term {
			rf.logger.Warn("discarding stale reply",
				"currentTerm", rf.currentTerm,
				"replyTerm", r.Term,
				"result", r,
			)
			rf.mu.Unlock()
			continue
		}

		if r.Success {
			// not a heartbeat
			if len(r.Entries) > 0 {
				mi := maxIndex(r.Entries)
				rf.logger.Debug("append entry success reply",
					"currentTerm", rf.currentTerm,
					"from", r.PeerId,
					"maxIndex", mi,
					"len", len(r.Entries),
					"nextIndex", rf.nextIndex[r.PeerId],
					"matchIndex", rf.matchIndex[r.PeerId],
				)
				rf.matchIndex[r.PeerId] = mi
				rf.nextIndex[r.PeerId] = mi + 1
			}

			rf.maybeCommitEntries()
		} else {
			// lagging follower conflict resolution
			if r.XTerm == -1 {
				// Case 3: Follower's log shorter than leader's
				rf.nextIndex[r.PeerId] = r.XLen + 1
			} else {
				lastXTermIndex := rf.findLastIndexForTerm(r.XTerm)
				if lastXTermIndex != -1 {
					// Case 2: Leader has XTerm entries
					rf.nextIndex[r.PeerId] = lastXTermIndex
				} else {
					// Case 1: Leader doesn't have XTerm
					rf.nextIndex[r.PeerId] = r.XIndex
				}
			}

			// send logs immediately
			// entries := rf.getEntriesForFollower(r.PeerId)
			// rf.replicateEnteriesToFollower(entries, r.PeerId)
		}

		rf.mu.Unlock()
	}

}

// needs to be called with rf.mu locked
func (rf *Raft) maybeCommitEntries() {
	var matchIndexes []int

	// always include the leader's own log length as if it were a matchIndex
	matchIndexes = append(matchIndexes, rf.getLogLen())
	for _, idx := range rf.matchIndex {
		matchIndexes = append(matchIndexes, idx)
	}
	sort.Ints(matchIndexes)
	// median of sorted match indexes is the same as majority
	majorityIdx := matchIndexes[len(matchIndexes)/2]
	if majorityIdx > rf.commitIndex {
		// check if term is still valid (fig 8)
		if rf.getLogEntry(majorityIdx).Term == rf.currentTerm {
			rf.logger.Info("advancing commitIndex",
				"old", rf.commitIndex,
				"new", max(rf.commitIndex, majorityIdx),
				"matchIndexes", matchIndexes,
			)
			rf.commitIndex = majorityIdx
		}
	}
}
