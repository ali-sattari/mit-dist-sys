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
func (rf *Raft) sendCommand(cmd any) uint {
	l := LogEntry{
		Id:      rf.logIndexes[len(rf.logs)-1] + 1,
		Term:    rf.currentTerm,
		Command: cmd,
	}
	rf.logs[l.Id] = l
	rf.logIndexes = append(rf.logIndexes, l.Id)

	rf.logger.Info("received a command",
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

	for i := range rf.peers {
		// skip the leader itself
		if i == rf.me {
			continue
		}

		entries := rf.getEntriesForFollower(i)

		// if not heartbeat
		if len(entries) > 0 {
			rf.logger.Debug(fmt.Sprintf("sending entry to %d", i),
				"nextIndex", rf.nextIndex[i],
				"matchIndex", rf.matchIndex[i],
			)
		} else {
			rf.logger.Debug(fmt.Sprintf("sending heartbeat to %d", i),
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
			} else {
				rf.logger.Debug("error in append entry rpc",
					"peer", server,
					"args", a,
					"entries", entries,
				)
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
	prevLogIndex := rf.nextIndex[peer] - 1
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
	have := map[uint]uint{}
	need := uint(len(rf.peers) / 2)

	for r := range rf.appendReplyCh {
		rf.mu.Lock()

		if rf.nodeRole != Leader {
			rf.logger.Debug("got append reply, not leader anymore",
				"have", have,
				"reply", r)
			rf.mu.Unlock()
			return
		}

		rf.logger.Debug("got append entry reply",
			"have", have,
			"commitIndex", rf.commitIndex,
			"reply", r)

		// step down if we get a higher term
		if rf.currentTerm < r.Response.Term {
			rf.logger.Warn("stepping down due to higher term",
				"current_term", rf.currentTerm,
				"new_term", r.Response.Term)
			rf.increaseTerm(r.Response.Term)
			rf.transition(Follower)
			rf.mu.Unlock()
			return
		}

		if r.Response.Success {
			// not a heartbeat
			if len(r.Entries) > 0 {
				mi := maxIndex(r.Entries)
				rf.logger.Debug("append entry success reply",
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
						rf.logger.Info("entry replicated, sending to app",
							"term", rf.currentTerm,
							"lastApplied", rf.lastApplied,
							"replies", have[e.Id],
							"entry", e,
						)

						rf.commitIndex = e.Id
						rf.lastApplied = e.Id

						rf.applyCh <- raftapi.ApplyMsg{
							CommandValid: true,
							Command:      e.Command,
							CommandIndex: int(e.Id),
						}
					}
				}
			}
		} else {
			// follower is behind, decrement nextIndex
			rf.nextIndex[r.Server]--
		}

		rf.mu.Unlock()
	}

}
