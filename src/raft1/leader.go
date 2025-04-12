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

	for i := range rf.peers {
		if i == int(rf.me) {
			continue
		}

		prevLogIndex := rf.matchIndex[i]
		a := AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.logs[prevLogIndex].Term,
			LeaderCommit: rf.commitIndex,
		}
		r := AppendEntryReply{}

		go func(server int) {
			rf.sendAppendEntry(server, &a, &r)
		}(i)
	}

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

	rf.sendLogEntry([]LogEntry{l})

	return l.Id
}

// needs to be called with rf.mu locked
func (rf *Raft) sendLogEntry(entries []LogEntry) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		prevLogIndex := rf.matchIndex[i]
		a := AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.logs[prevLogIndex].Term,
			LeaderCommit: rf.commitIndex,
			Entries:      entries,
		}
		r := AppendEntryReply{}

		rf.nextIndex[i] = entries[len(entries)-1].Id

		rf.logger.Debug(fmt.Sprintf("sending AppendEntry to %d", i),
			"nextIndex", rf.nextIndex[i],
			"matchIndex", rf.matchIndex[i],
		)

		go func(server int) {
			if ok := rf.sendAppendEntry(server, &a, &r); ok {
				rf.appendReplyCh <- AppendEntryResult{
					server:   server,
					entries:  entries,
					response: r,
				}
			} else {
				rf.logger.Error("error sending append entry rpc",
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
	sent := map[uint]bool{}

	for r := range rf.appendReplyCh {
		rf.mu.Lock()

		rf.logger.Debug("got append entry reply",
			"reply", r)

		// step down if we get a higher term
		if rf.currentTerm < r.response.Term {
			rf.logger.Debug("stepping down due to higher term",
				"current_term", rf.currentTerm,
				"new_term", r.response.Term)
			rf.increaseTerm(r.response.Term)
			rf.transition(Follower)
			rf.mu.Unlock()
			return
		}

		if r.response.Success {
			for _, e := range r.entries {
				rf.matchIndex[r.server] = e.Id
				have[e.Id]++

				if !sent[e.Id] && have[e.Id] >= need {
					rf.logger.Debug("append entry replicated to majority",
						"term", rf.currentTerm,
						"replies", have[e.Id])

					sent[e.Id] = true // send apply only once
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
			// decrement nextIndex and retry
			// rf.nextIndex[r.server]--
			// ls := []LogEntry{}
			// for _, l := range rf.logIndexes[rf.nextIndex[r.server]:] {
			// 	ls = append(ls, rf.logs[l])
			// }
			// go rf.sendLogEntry(ls)
		}

		rf.mu.Unlock()
	}
}
