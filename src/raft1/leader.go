package raft

import "time"

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

		prevLogIndex := rf.matchIndex[uint(i)]
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
			// TODO: handle reply in a channel
		}(i)
	}

	rf.lastBeat = time.Now()
}
