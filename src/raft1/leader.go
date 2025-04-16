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
func (rf *Raft) sendHeartbeat() {
	// check if it is time to send heartbeat
	if time.Since(rf.lastBeat) < heartbeatInterval {
		return
	}

	rf.logger.Debug("time for heartbeat")
	rf.sendLogEntry([]LogEntry{})
	rf.lastBeat = time.Now()
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

	rf.logger.Debug("received a command",
		"entry", l,
	)

	rf.sendLogEntry([]LogEntry{l})

	return l.Id
}

// needs to be called with rf.mu locked
func (rf *Raft) makeAppendEntryArgs(peer int, entries []LogEntry) AppendEntryArgs {
	prevLogIndex := rf.matchIndex[peer]
	return AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
}

// needs to be called with rf.mu locked
func (rf *Raft) sendLogEntry(entries []LogEntry) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// if not heartbeat
		if len(entries) > 0 {
			// is follower lagging?
			var mid uint
			for _, e := range entries {
				mid = max(mid, e.Id)
			}
			if mid-rf.matchIndex[i] > 1 {
				ls := []LogEntry{}
				for _, l := range rf.logIndexes[rf.nextIndex[i]:] {
					ls = append(ls, rf.logs[l])
				}
				entries = ls
				rf.logger.Debug("follower lagging",
					"peer", i,
					"nextIndex", rf.nextIndex[i],
					"matchIndex", rf.matchIndex[i],
					"sending", ls)
			}

			rf.logger.Debug(fmt.Sprintf("sending AppendEntry to %d", i),
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
				rf.logger.Debug("error sending append entry rpc",
					"peer", server,
					"args", a,
					"entries", entries,
				)
			}
		}(i)
	}
}

func (rf *Raft) waitForAppendReply() {
	have := map[uint]uint{}
	need := uint(len(rf.peers) / 2)

	for r := range rf.appendReplyCh {
		rf.mu.Lock()

		if rf.nodeRole != Leader {
			rf.logger.Debug("got append entry reply but not a leader anymore!",
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
			rf.logger.Debug("stepping down due to higher term",
				"current_term", rf.currentTerm,
				"new_term", r.Response.Term)
			rf.increaseTerm(r.Response.Term)
			rf.transition(Follower)
			rf.mu.Unlock()
			return
		}

		if r.Response.Success {
			for _, e := range r.Entries {
				rf.matchIndex[r.Server] = max(e.Id, rf.matchIndex[r.Server])
				rf.nextIndex[r.Server] = max(e.Id, rf.nextIndex[r.Server])
				have[e.Id]++

				if have[e.Id] >= need && rf.commitIndex < e.Id {
					rf.logger.Debug("append entry replicated to majority, sending to app",
						"term", rf.currentTerm,
						"replies", have[e.Id])

					rf.commitIndex = e.Id
					rf.lastApplied = e.Id

					rf.applyCh <- raftapi.ApplyMsg{
						CommandValid: true,
						Command:      e.Command,
						CommandIndex: int(e.Id),
					}
				}
			}
		} else {
			// follower is behind, decrement nextIndex and retry
			rf.nextIndex[r.Server]--
			ls := []LogEntry{}
			for _, l := range rf.logIndexes[rf.nextIndex[r.Server]:] {
				ls = append(ls, rf.logs[l])
			}
			go rf.sendLogEntry(ls)
		}

		rf.mu.Unlock()
	}
}
