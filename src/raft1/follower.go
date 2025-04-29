package raft

import (
	"time"
)

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Debug("append entry request",
		"currentTerm", rf.currentTerm,
		"args", args,
		// "logs", rf.logs,
		// "logIndexs", rf.logIndexes,
	)

	// bad cases
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term { // 5.1
		rf.logger.Info("rejecting entry, outdated leader",
			"currentTerm", rf.currentTerm,
			"leader", args.LeaderId,
			"leaderTerm", args.Term)
		reply.Success = false
		return
	}

	// only reset if rpc is valid
	rf.lastBeat = time.Now()
	rf.resetVotedFor()

	// fig 2: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	l, ok := rf.logs[args.PrevLogIndex]
	if !ok {
		rf.logger.Info("rejecting entry, log length mismatch",
			"currentTerm", rf.currentTerm,
			"args", args,
			"len", len(rf.logs),
			"logs", rf.logs,
		)
		reply.Success = false
		reply.XLen = len(rf.logs)
		return
	}
	if l.Term != args.PrevLogTerm {
		rf.logger.Info("rejecting entry, log term mismatch",
			"currentTerm", rf.currentTerm,
			"args", args,
			"logs", rf.logs,
		)
		reply.Success = false
		reply.XTerm = rf.logs[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.logs[i].Term != reply.XTerm {
				reply.XIndex = i + 1
				break
			}
		}
		return
	}

	// good case
	if rf.nodeRole == Candidate {
		rf.logger.Info("stepping down, got rpc",
			"currentTerm", rf.currentTerm,
			"leader", args.LeaderId)
		rf.transition(Follower)
	}

	// append entries
	for _, e := range args.Entries {
		if l, ok := rf.logs[e.Id]; ok {
			if l.Term != e.Term { // 5.3
				rf.logger.Info("deleting log entries",
					"currentTerm", rf.currentTerm,
					"fromIndex", e.Id)
				rf.deleteLogEntries(e.Id)
			}
		}

		rf.addEntryToLog(e)
	}

	// update commit index
	if rf.commitIndex < args.LeaderCommit {
		ll := rf.getLastLogEntry()
		rf.commitIndex = min(args.LeaderCommit, ll.Id)
	}

	if rf.nodeRole != Follower {
		rf.logger.Info("stepping down, higher term",
			"currentTerm", rf.currentTerm,
			"newTerm", args.Term,
			"leader", args.LeaderId)
		rf.transition(Follower)
	}
	rf.increaseTerm(args.Term)

	reply.Term = rf.currentTerm
	reply.Success = true

	// rf.logger.Debug("processed append entry",
	// 	"commitIndex", rf.commitIndex,
	// 	"lastApplied", rf.lastApplied,
	// 	// "logIndexs", rf.logIndexes,
	// )
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Debug("vote request",
		"currentTerm", rf.currentTerm,
		"args", args)

	if rf.currentTerm > args.Term { // out-of-date candidate
		rf.logger.Warn("rejecting vote, outdated candidate",
			"currentTerm", rf.currentTerm,
			"candidate", args.CandidateId,
			"candidateTerm", args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term { // got a higher term!
		if rf.nodeRole != Follower {
			rf.logger.Info("stepping down, higher term",
				"currentTerm", rf.currentTerm,
				"newTerm", args.Term,
				"candidate", args.CandidateId)
			rf.transition(Follower)
		}
		rf.increaseTerm(args.Term)
	}

	reply.Term = rf.currentTerm

	ll := rf.getLastLogEntry()
	if args.LastLogTerm > ll.Term ||
		(args.LastLogTerm == ll.Term && args.LastLogIndex >= ll.Id) {
		if rf.votedFor == -1 {
			rf.logger.Info("granting vote",
				"currentTerm", rf.currentTerm,
				"candidate", args.CandidateId)
			reply.VoteGranted = true
			rf.castVote(args.CandidateId)

			// only reset timer when grating vote, not on other cases
			// from https://thesquareplanet.com/blog/students-guide-to-raft/
			rf.lastBeat = time.Now()
		} else {
			rf.logger.Debug("already voted",
				"currentTerm", rf.currentTerm,
				"votedFor", rf.votedFor,
				"candidate", args.CandidateId)
			reply.VoteGranted = (rf.votedFor == args.CandidateId)
		}
	} else {
		rf.logger.Warn("rejecting vote, log mismatch",
			"currentTerm", rf.currentTerm,
			"follower", ll,
			"args", args)
		reply.VoteGranted = false
	}
}

// needs to be called with rf.mu locked
func (rf *Raft) deleteLogEntries(from int) {
	f := rf.logIndexes[:0]
	for _, idx := range rf.logIndexes {
		if idx < from {
			f = append(f, idx)
		} else {
			delete(rf.logs, idx)
		}
	}
	rf.logIndexes = f

	rf.persist()
}
