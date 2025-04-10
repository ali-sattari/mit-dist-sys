package raft

import (
	"time"
)

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Debug("append entry request",
		"server", rf.me,
		"args", args)

	rf.lastBeat = time.Now()
	rf.votedFor = nil

	// bad cases
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term { // 5.1
		rf.logger.Debug("rejecting append entry from outdated leader",
			"server", rf.me,
			"current_term", rf.currentTerm,
			"leader_term", args.Term)
		reply.Success = false
		return
	}

	if l, ok := rf.logs[args.PrevLogIndex]; !ok || l.Term != args.PrevLogTerm { // 5.3
		rf.logger.Debug("rejecting append entry due to log inconsistency",
			"server", rf.me,
			"index", args.PrevLogIndex)
		reply.Success = false
		return
	}

	// good case
	if rf.nodeState == Candidate {
		rf.logger.Debug("stepping down from candidate to follower",
			"server", rf.me,
			"leader", args.LeaderId)
		rf.transition(Follower)
	}

	// apply entries
	for _, e := range args.Entries {
		if l, ok := rf.logs[e.Id]; ok {
			if l.Term != e.Term { // 5.3
				rf.logger.Debug("deleting conflicting log entries",
					"server", rf.me,
					"from_index", e.Id)
				rf.deleteLogEntries(e.Id)
			}
		}

		rf.logs[e.Id] = e
		rf.logIndexes = append(rf.logIndexes, e.Id)
	}

	// update commit index
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

	rf.logger.Debug("vote request",
		"server", rf.me,
		"args", args)

	rf.lastBeat = time.Now()

	if rf.currentTerm > args.Term { // out-of-date candidate
		rf.logger.Debug("rejecting vote request from outdated candidate",
			"server", rf.me,
			"current_term", rf.currentTerm,
			"candidate_term", args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term { // got a higher term!
		rf.logger.Debug("stepping down due to higher term",
			"server", rf.me,
			"current_term", rf.currentTerm,
			"new_term", args.Term,
			"candidate", args.CandidateId)
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.transition(Follower)
	}

	reply.Term = rf.currentTerm
	if (rf.votedFor == nil || rf.votedFor == &args.CandidateId) &&
		(rf.lastApplied <= args.LastLogIndex) {
		rf.logger.Debug("granting vote to candidate",
			"server", rf.me,
			"candidate", args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = &args.CandidateId
	} else {
		rf.logger.Debug("rejecting vote request",
			"server", rf.me,
			"candidate", args.CandidateId)
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
