package raft

import (
	"fmt"
	"math/rand"
	"slices"
	"time"
)

type NodeState string

const (
	Follower  NodeState = "follower"
	Candidate           = "candidate"
	Leader              = "leader"
)

const ElectionTimeout time.Duration = time.Millisecond * 500

// Transition table
var validTransitions = map[NodeState][]NodeState{
	Follower:  {Candidate},
	Candidate: {Follower, Leader},
	Leader:    {Follower},
}

func (rf *Raft) transition(newState NodeState) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Validate transition
	if !slices.Contains(validTransitions[rf.nodeState], newState) {
		return fmt.Errorf("invalid transition %s -> %s", rf.nodeState, newState)
	}

	// State-specific cleanup/init
	switch newState {
	case Leader:
		// initialize state
		// start heartbeat ticker
	case Candidate:
		// incr term
		// vote for self
		// send RequestVote RPCs
	case Follower:
		// start election/comms timeout timer
	}

	rf.nodeState = newState
	return nil
}

// State machine core
func (rf *Raft) ticker() {
	for !rf.killed() {
		// TODO: Your code here (3A)
		// Check if a leader election should be started.
		switch rf.nodeState {
		// Rlock or switch to channels?
		case Follower:
			if rf.electionTimedout() {
				rf.transition(Candidate)
			}

		case Candidate:
			if rf.votedFor == nil || rf.electionTimedout() {
				rf.startElection()
				go rf.waitForVotes()
			}

		case Leader:
			rf.sendHeartbeat()
		}

		// pause for a random amount of time between 50 and 350 milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) electionTimedout() bool {
	return rf.lastBeat.Before(time.Now().Add(-ElectionTimeout))
}
