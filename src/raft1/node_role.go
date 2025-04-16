package raft

import (
	"fmt"
	"math/rand"
	"slices"
	"time"
)

type NodeRole string

const (
	Follower  NodeRole = "follower"
	Candidate NodeRole = "candidate"
	Leader    NodeRole = "leader"
)

const electionTimeout = 250 // milliseconds
const electionJitter = 100  // milliseconds
const stateLoopInterval = time.Millisecond * 10
const heartbeatInterval = time.Millisecond * 100

// Transition table
var validTransitions = map[NodeRole][]NodeRole{
	Follower:  {Candidate},
	Candidate: {Follower, Leader},
	Leader:    {Follower},
}

// needs to be called with rf.mu locked
func (rf *Raft) transition(newState NodeRole) error {
	if newState == rf.nodeRole {
		return nil
	}

	// validate transition
	if !slices.Contains(validTransitions[rf.nodeRole], newState) {
		rf.logger.Error("invalid state transition",
			"from", rf.nodeRole,
			"to", newState)
		return fmt.Errorf("invalid transition %s -> %s", rf.nodeRole, newState)
	}

	rf.logger.Debug("state transition",
		"from", rf.nodeRole,
		"to", newState)

	rf.nodeRole = newState
	rf.setupLogging() // dirty trick, but hey it works

	return nil
}

// State machine core
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		switch rf.nodeRole {
		case Follower:
			if rf.isElectionTimedout() {
				rf.logger.Debug("election time out",
					"lastBeat", time.Until(rf.lastBeat),
					"term", rf.currentTerm,
				)
				rf.transition(Candidate)
			}

		case Candidate:
			if rf.needsElection() {
				rf.startElection()
				go rf.waitForVotes()
			}

		case Leader:
			rf.sendHeartbeat()
			go rf.waitForAppendReply()
			// TODO: set nextIndex to leader's last index+1
		}

		rf.mu.Unlock()
		time.Sleep(stateLoopInterval)
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
	r := electionTimeout + (rand.Int63() % electionJitter)
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
