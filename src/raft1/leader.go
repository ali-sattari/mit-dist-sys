package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// needs to be called with rf.mu locked
func (rf *Raft) sendCommand(cmd any) int {
	l := LogEntry{
		Id:      rf.logIndexes[len(rf.logs)-1] + 1,
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

		entries := rf.getEntriesForFollower(i)

		// if not heartbeat
		if len(entries) > 0 {
			rf.logger.Debug(fmt.Sprintf("sending entry to %d", i),
				"currentTerm", rf.currentTerm,
				"nextIndex", rf.nextIndex[i],
				"matchIndex", rf.matchIndex[i],
			)
		} else {
			rf.logger.Debug(fmt.Sprintf("sending heartbeat to %d", i),
				"currentTerm", rf.currentTerm,
				"nextIndex", rf.nextIndex[i],
				"matchIndex", rf.matchIndex[i],
			)
		}

		rf.replicateEnteriesToFollower(entries, i)
	}
}

// needs to be called with rf.mu locked
func (rf *Raft) getEntriesForFollower(server int) []LogEntry {
	ls := []LogEntry{}
	for _, l := range rf.logIndexes[rf.nextIndex[server]:] {
		ls = append(ls, rf.logs[l])
	}
	return ls
}

// needs to be called with rf.mu locked
func (rf *Raft) setFollowerIndexes() {
	li := rf.logIndexes[len(rf.logIndexes)-1]
	for i := range rf.peers {
		rf.nextIndex[i] = li + 1
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
		PrevLogTerm:  rf.logs[prevLogIndex].Term,
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

func (rf *Raft) receiveAppendReply() {
	have := map[int]int{}
	need := int(len(rf.peers) / 2)

	for r := range rf.appendReplyCh {
		rf.mu.Lock()

		if rf.nodeRole != Leader {
			rf.logger.Debug("got append reply, not leader anymore",
				"currentTerm", rf.currentTerm,
				"commitIndex", rf.commitIndex,
				"reply", r,
				"have", have,
			)
			rf.mu.Unlock()
			return
		}

		rf.logger.Debug("got append entry reply",
			"currentTerm", rf.currentTerm,
			"commitIndex", rf.commitIndex,
			"reply", r,
			"have", have,
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
					"nextIndex", rf.nextIndex[r.PeerId],
					"matchIndex", rf.matchIndex[r.PeerId],
				)
				rf.matchIndex[r.PeerId] = mi
				rf.nextIndex[r.PeerId] = mi + 1

				for _, e := range r.Entries {
					have[e.Id]++

					if have[e.Id] >= need && rf.commitIndex < e.Id {
						// check if term is still valid (fig 8)
						if rf.currentTerm != e.Term {
							rf.logger.Warn("term mismatch pre-apply",
								"currentTerm", rf.currentTerm,
								"peer", r.PeerId,
								"commitIndex", rf.commitIndex,
								"lastApplied", rf.lastApplied,
								"entry", e,
							)

							continue
						}

						rf.logger.Info("advancing commitIndex",
							"old", rf.commitIndex,
							"new", max(rf.commitIndex, e.Id),
						)
						rf.commitIndex = max(rf.commitIndex, e.Id)
					}
				}
			}
		} else {
			// lagging follower conflict resolution
			if r.XLen > 0 {
				// Case 3: Follower's log shorter than leader's
				rf.nextIndex[r.PeerId] = r.XLen
			} else {
				// Find last occurrence of XTerm in leader's log
				lastXTermIndex := rf.findFirstIndexForTerm(r.XTerm)

				if lastXTermIndex != -1 {
					// Case 2: Leader has XTerm entries
					rf.nextIndex[r.PeerId] = lastXTermIndex + 1
				} else {
					// Case 1: Leader doesn't have XTerm
					rf.nextIndex[r.PeerId] = r.XIndex
				}
			}

			// follower is behind, decrement nextIndex
			// rf.nextIndex[r.PeerId] = max(0, rf.nextIndex[r.PeerId]-1)

			// send logs immediately
			entries := rf.getEntriesForFollower(r.PeerId)
			rf.replicateEnteriesToFollower(entries, r.PeerId)
		}

		rf.mu.Unlock()
	}

}
