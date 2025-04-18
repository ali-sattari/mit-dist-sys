package raft

import (
	"fmt"
	"time"

	"6.5840/raftapi"
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
	rf.logs[l.Id] = l
	rf.logIndexes = append(rf.logIndexes, l.Id)

	rf.logger.Info("received a command",
		"currentTerm", rf.currentTerm,
		"entry", l,
	)

	return l.Id
}

// needs to be called with rf.mu locked
func (rf *Raft) replicateLogEntries() {
	// check if it is time to send heartbeat or entries
	if time.Since(rf.lastBeat) < heartbeatInterval {
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

		a := rf.makeAppendEntryArgs(i, entries)
		r := AppendEntryReply{}

		go func(server int) {
			if ok := rf.sendAppendEntry(server, &a, &r); ok {
				rf.appendReplyCh <- AppendEntryResult{
					Server:   server,
					Entries:  entries,
					Response: r,
				}
			}
		}(i)
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
func (rf *Raft) makeAppendEntryArgs(peer int, entries []LogEntry) AppendEntryArgs {
	prevLogIndex := max(0, rf.nextIndex[peer]-1)
	return AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
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
		if rf.currentTerm < r.Response.Term {
			rf.logger.Warn("stepping down due to higher term",
				"currentTerm", rf.currentTerm,
				"newTerm", r.Response.Term)
			rf.increaseTerm(r.Response.Term)
			rf.transition(Follower)
			rf.mu.Unlock()
			return
		}

		// reject response from an older term
		if rf.currentTerm != r.Response.Term {
			rf.logger.Warn("discarding stale reply",
				"currentTerm", rf.currentTerm,
				"replyTerm", r.Response.Term,
				"result", r,
			)
			rf.mu.Unlock()
			return
		}

		if r.Response.Success {
			// not a heartbeat
			if len(r.Entries) > 0 {
				mi := maxIndex(r.Entries)
				rf.logger.Debug("append entry success reply",
					"currentTerm", rf.currentTerm,
					"from", r.Server,
					"maxIndex", mi,
					"nextIndex", rf.nextIndex[r.Server],
					"matchIndex", rf.matchIndex[r.Server],
				)
				rf.matchIndex[r.Server] = mi
				rf.nextIndex[r.Server] = mi + 1

				for _, e := range r.Entries {
					have[e.Id]++

					if have[e.Id] >= need && rf.commitIndex < e.Id {
						// check if term is still valid (fig 8)
						if rf.currentTerm != e.Term {
							rf.logger.Warn("term mismatch pre-apply",
								"currentTerm", rf.currentTerm,
								"peer", r.Server,
								"commitIndex", rf.commitIndex,
								"lastApplied", rf.lastApplied,
								"entry", e,
							)
						}

						rf.commitIndex = max(rf.commitIndex, e.Id)
					}
				}
			}

			// send committed logs to apply chan
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastApplied].Command,
					CommandIndex: int(rf.lastApplied),
				}
				rf.logger.Info("entry replicated, sending to app",
					"currentTerm", rf.currentTerm,
					"lastApplied", rf.lastApplied,
					"entry", rf.logs[rf.lastApplied],
				)
			}
		} else {
			// follower is behind, decrement nextIndex
			rf.nextIndex[r.Server] = max(0, rf.nextIndex[r.Server]-1)
		}

		rf.mu.Unlock()
	}

}
