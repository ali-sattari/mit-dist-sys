package raft

import "time"

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	rf.votedFor = &rf.me
	rf.lastBeat = time.Now()

	for i := range rf.peers {
		if i == int(rf.me) {
			continue
		}

		li := rf.logs[rf.lastApplied]
		a := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: li.id,
			LastLogTerm:  li.term,
		}
		r := RequestVoteReply{}

		go func() {
			rf.sendRequestVote(i, &a, &r)
			rf.voteCh <- r
		}()
	}
}

func (rf *Raft) waitForVotes() {
	votes := 0
	need := int(len(rf.peers)/2) + 1

	for r := range rf.voteCh {
		if rf.currentTerm == r.Term && r.VoteGranted {
			votes++
		}

		if votes >= need {
			rf.transition(Leader)
			break
		}
	}
}
