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
	Candidate NodeState = "candidate"
	Leader    NodeState = "leader"
)

const electionTimeout = 300 // milliseconds
const stateLoopTime = time.Nanosecond * 1000
const heartbeatInterval = time.Millisecond * 100

// Transition table
var validTransitions = map[NodeState][]NodeState{
	Follower:  {Candidate},
	Candidate: {Follower, Leader},
	Leader:    {Follower},
}

// needs to be called with rf.mu locked
func (rf *Raft) transition(newState NodeState) error {
	if newState == rf.nodeState {
		return nil
	}

	// validate transition
	if !slices.Contains(validTransitions[rf.nodeState], newState) {
		rf.logger.Error("invalid state transition",
			"server", rf.me,
			"from", rf.nodeState,
			"to", newState)
		return fmt.Errorf("invalid transition %s -> %s", rf.nodeState, newState)
	}

	rf.logger.Debug("state transition",
		"server", rf.me,
		"from", rf.nodeState,
		"to", newState)

	rf.nodeState = newState
	return nil
}

// State machine core
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		switch rf.nodeState {
		case Follower:
			if rf.isElectionTimedout() {
				rf.transition(Candidate)
			}

		case Candidate:
			if rf.needsElection() {
				rf.startElection()
				go rf.waitForVotes()
			}

		case Leader:
			rf.sendHeartbeat()
		}

		rf.mu.Unlock()
		time.Sleep(stateLoopTime)
	}
}

func (rf *Raft) receiveBeats() {
	for range rf.beatCh {
		rf.mu.Lock()
		rf.lastBeat = time.Now()
		rf.mu.Unlock()
	}
}

// needs to be called with rf.mu locked
func (rf *Raft) isElectionTimedout() bool {
	// the randomness added for election time checking
	r := 50 + (rand.Int63() % electionTimeout)
	t := time.Duration(r) * time.Millisecond
	return rf.lastBeat.Before(time.Now().Add(-t))
}

// needs to be called with rf.mu locked
func (rf *Raft) increaseTerm(newTerm uint) {
	rf.currentTerm = newTerm
	rf.votedFor = nil
}

// needs to be called with rf.mu locked
func (rf *Raft) needsElection() bool {
	return rf.votedFor == nil || rf.isElectionTimedout()
}
