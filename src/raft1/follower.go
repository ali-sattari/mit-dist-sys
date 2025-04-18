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
	rf.votedFor = nil

	// fig 2: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if l, ok := rf.logs[args.PrevLogIndex]; !ok || l.Term != args.PrevLogTerm {
		rf.logger.Info("rejecting entry, log inconsistency",
			"currentTerm", rf.currentTerm,
			"args", args,
			"logs", rf.logs,
		)
		reply.Success = false
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

		rf.logs[e.Id] = e
		rf.logIndexes = append(rf.logIndexes, e.Id)
	}

	// update commit index
	if rf.commitIndex < args.LeaderCommit {
		ll := rf.getLastLogEntry()
		rf.commitIndex = min(args.LeaderCommit, ll.Id)
	}

	// send committed to apply chan
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: int(rf.lastApplied),
		}
		rf.logger.Debug("sent committed to app",
			"currentTerm", rf.currentTerm,
			"commitIndex", rf.commitIndex,
			"lastApplied", rf.lastApplied,
			"entry", rf.logs[rf.lastApplied],
		)
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

	if rf.nodeRole == Leader {
		// no need to further process this lagging request
		reply.VoteGranted = false
		return
	}

	ll := rf.getLastLogEntry()
	var vf string
	if rf.votedFor != nil {
		vf = fmt.Sprintf("%v", *rf.votedFor)
	}
	if rf.votedFor == nil || rf.votedFor == &args.CandidateId {
		if args.LastLogTerm > ll.Term ||
			(args.LastLogTerm == ll.Term && args.LastLogIndex >= ll.Id) {
			rf.logger.Info("granting vote",
				"currentTerm", rf.currentTerm,
				"candidate", args.CandidateId)
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId

			// only reset timer when grating vote, not on other cases
			// from https://thesquareplanet.com/blog/students-guide-to-raft/
			rf.lastBeat = time.Now()
		} else {
			rf.logger.Warn("rejecting vote, log mismatch",
				"currentTerm", rf.currentTerm,
				"follower", ll,
				"args", args)
			reply.VoteGranted = false
		}
	} else {
		rf.logger.Debug("already voted",
			"currentTerm", rf.currentTerm,
			"votedFor", vf,
			"candidate", args.CandidateId)
		reply.VoteGranted = false
	}
}

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
}
