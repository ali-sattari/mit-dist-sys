package raft

import (
	"fmt"
	"time"

	"6.5840/raftapi"
)

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Debug("append entry request",
		"args", args,
		// "logs", rf.logs,
		// "logIndexs", rf.logIndexes,
	)

	rf.lastBeat = time.Now()
	rf.votedFor = nil

	// bad cases
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term { // 5.1
		rf.logger.Info("rejecting entry, outdated leader",
			"current_term", rf.currentTerm,
			"leader_term", args.Term)
		reply.Success = false
		return
	}

	if l, ok := rf.logs[args.PrevLogIndex]; !ok || l.Term != args.PrevLogTerm { // 5.3
		rf.logger.Info("rejecting entry, log inconsistency",
			"args", args,
			"logs", rf.logs,
		)
		reply.Success = false
		return
	}

	// good case
	if rf.nodeRole == Candidate {
		rf.logger.Info("stepping down, got rpc",
			"leader", args.LeaderId)
		rf.transition(Follower)
	}

	// append entries
	for _, e := range args.Entries {
		if l, ok := rf.logs[e.Id]; ok {
			if l.Term != e.Term { // 5.3
				rf.logger.Info("deleting log entries",
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

	// send committed to apply chan
	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: int(rf.lastApplied),
		}
		rf.logger.Debug("sent committed to app",
			"commitIndex", rf.commitIndex,
			"lastApplied", rf.lastApplied,
			"entry", rf.logs[rf.lastApplied],
		)
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	reply.Success = true

	// rf.logger.Debug("processed append entry",
	// 	"commitIndex", rf.commitIndex,
	// 	"lastApplied", rf.lastApplied,
	// 	// "logIndexs", rf.logIndexes,
	// )
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO: Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Debug("vote request",
		"term", rf.currentTerm,
		"args", args)

	rf.lastBeat = time.Now()

	if rf.currentTerm > args.Term { // out-of-date candidate
		rf.logger.Warn("rejecting vote, outdated candidate",
			"current_term", rf.currentTerm,
			"candidate_term", args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term { // got a higher term!
		if rf.nodeRole != Follower {
			rf.logger.Info("stepping down, higher term",
				"current_term", rf.currentTerm,
				"new_term", args.Term,
				"candidate", args.CandidateId)
			rf.transition(Follower)
		}
		rf.increaseTerm(args.Term)
	}

	reply.Term = rf.currentTerm
	ll := rf.getLastLogEntry()
	var vf string
	if rf.votedFor != nil {
		vf = fmt.Sprintf("%v", *rf.votedFor)
	}
	if rf.votedFor == nil || rf.votedFor == &args.CandidateId {
		if args.LastLogTerm > ll.Term ||
			(args.LastLogTerm == ll.Term && args.LastLogIndex >= ll.Id) {
			rf.logger.Info("granting vote",
				"candidate", args.CandidateId)
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId

		} else {
			rf.logger.Warn("rejecting vote, log mismatch",
				"follower", ll,
				"candidate", args)
			reply.VoteGranted = false
		}
	} else {
		rf.logger.Debug("already voted",
			"votedFor", vf,
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
