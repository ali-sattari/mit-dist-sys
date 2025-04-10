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

	term := rf.currentTerm
	candidateId := rf.me
	lastApplied := rf.lastApplied
	lastLogTerm := rf.logs[lastApplied].Term

	for i := range rf.peers {
		if i == int(rf.me) {
			continue
		}

		a := RequestVoteArgs{
			Term:         term,
			CandidateId:  candidateId,
			LastLogIndex: lastApplied,
			LastLogTerm:  lastLogTerm,
		}
		r := RequestVoteReply{}

		go func(server int) {
			rf.sendRequestVote(server, &a, &r)
			rf.voteCh <- r
		}(i)
	}
}

func (rf *Raft) waitForVotes() {
	votes := 1 // Start with 1 vote (self)
	need := int(len(rf.peers)/2) + 1

	for r := range rf.voteCh {
		rf.mu.Lock()
		term := rf.currentTerm

		rf.logger.Debug("waiting for votes",
			"server", rf.me,
			"term", term,
			"needed", need,
			"current", votes,
			"reply", r)

		if term == r.Term && r.VoteGranted {
			votes++
			if votes >= need {
				rf.logger.Debug("election won",
					"server", rf.me,
					"term", term,
					"votes", votes)
				rf.transition(Leader)
				rf.mu.Unlock()
				return
			}
		}

		// step down if we get a higher term
		if term < r.Term {
			rf.logger.Debug("stepping down due to higher term",
				"server", rf.me,
				"current_term", term,
				"new_term", r.Term)
			rf.increaseTerm(r.Term)
			rf.transition(Follower)
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
	}
}
