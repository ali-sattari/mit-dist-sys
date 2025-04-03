package raft

import "time"

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastBeat = time.Now()

	// bad cases
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term { // 5.1
		reply.Success = false
		return
	}

	if l, ok := rf.logs[args.PrevLogIndex]; !ok || l.term != args.PrevLogTerm { // 5.3
		reply.Success = false
		return
	}

	// good case
	if rf.nodeState == Candidate {
		rf.transition(Follower)
	}

	for _, e := range args.Entries {
		if l, ok := rf.logs[e.id]; ok {
			if l.term != e.term { // 5.3
				rf.deleteLogEntries(e.id)
			}
		}

		rf.logs[e.id] = e
		rf.logIndexes = append(rf.logIndexes, e.id)
	}
	if rf.commitIndex < args.LeaderCommit {
		li := rf.logIndexes[len(rf.logIndexes)-1]
		rf.commitIndex = min(args.LeaderCommit, li)
	}
	rf.currentTerm = args.Term

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO: Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastBeat = time.Now()

	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term { // out-of-date candidate
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term { // got a higher term!
		rf.currentTerm = args.Term
		rf.transition(Follower)
	}

	if (rf.votedFor == nil || rf.votedFor == &args.CandidateId) &&
		(rf.lastApplied <= args.LastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = &args.CandidateId
	} else {
		reply.VoteGranted = false
	}

}

func (rf *Raft) deleteLogEntries(from uint) {
	f := rf.logIndexes[:0]
	for _, idx := range rf.logIndexes {
		if idx < from {
			f = append(f, idx)
		} else {
			delete(rf.logs, idx)
		}
	}
	rf.logIndexes = f
}
