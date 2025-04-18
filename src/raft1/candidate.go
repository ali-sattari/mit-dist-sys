package raft

import (
	"time"
)

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// needs to be called with rf.mu locked
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = &rf.me
	rf.lastBeat = time.Now()

	ll := rf.getLastLogEntry()

	rf.logger.Info("starting election",
		"currentTerm", rf.currentTerm,
	)

	for i := range rf.peers {
		if i == int(rf.me) {
			continue
		}

		a := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: ll.Id,
			LastLogTerm:  ll.Term,
		}
		r := RequestVoteReply{}

		go func(server int) {
			if ok := rf.sendRequestVote(server, &a, &r); ok {
				rf.voteReplyCh <- r
			}
		}(i)
	}
}

func (rf *Raft) waitForVotes() {
	votes := 1 // Start with 1 vote (self)
	need := int(len(rf.peers)/2) + 1

	for r := range rf.voteReplyCh {
		rf.mu.Lock()

		if rf.nodeRole != Candidate {
			rf.logger.Debug("got vote reply, not candidate anymore",
				"currentTerm", rf.currentTerm,
				"have", votes,
				"reply", r)
			rf.mu.Unlock()
			return
		}

		rf.logger.Debug("waiting for votes",
			"currentTerm", rf.currentTerm,
			"needed", need,
			"current", votes,
			"reply", r)

		if rf.currentTerm == r.Term && r.VoteGranted {
			votes++
			if votes >= need {
				rf.logger.Info("election won",
					"currentTerm", rf.currentTerm,
					"votes", votes)
				rf.transition(Leader)
				rf.mu.Unlock()
				return
			}
		}

		// step down if we get a higher term
		if rf.currentTerm < r.Term {
			rf.logger.Warn("stepping down due to higher term",
				"currentTerm", rf.currentTerm,
				"newTerm", r.Term)
			rf.increaseTerm(r.Term)
			rf.transition(Follower)
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
	}
}
