package raft

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat() {
	for i := range rf.peers {
		if i == int(rf.me) {
			continue
		}

		li := rf.matchIndex[uint(i)]
		a := AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: li, //right? or should it be leader's last index?
			PrevLogTerm:  rf.logs[li].term,
			LeaderCommit: rf.commitIndex,
		}
		r := AppendEntryReply{}

		rf.sendAppendEntry(i, &a, &r)
	}
}
