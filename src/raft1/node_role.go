package raft

import (
	"fmt"
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
const electionJitter = 900  // milliseconds
const stateLoopInterval = time.Millisecond * 10
const heartbeatInterval = time.Millisecond * 100

// Transition table
var validTransitions = map[NodeRole][]NodeRole{
	Follower:  {Candidate},
	Candidate: {Follower, Leader},
	Leader:    {Follower},
}

func ParseNodeRole(s string) (NodeRole, error) {
	switch s {
	case string(Follower):
		return Follower, nil
	case string(Candidate):
		return Candidate, nil
	case string(Leader):
		return Leader, nil
	default:
		return "", fmt.Errorf("invalid NodeRole: %q", s)
	}
}

// needs to be called with rf.mu locked
func (rf *Raft) transition(newState NodeRole) error {
	if newState == rf.nodeRole {
		return nil
	}

	// validate transition
	if !slices.Contains(validTransitions[rf.nodeRole], newState) {
		rf.logger.Error("invalid state transition",
			"currentTerm", rf.currentTerm,
			"from", rf.nodeRole,
			"to", newState)
		return fmt.Errorf("invalid transition %s -> %s", rf.nodeRole, newState)
	}

	rf.logger.Info("state transition",
		"currentTerm", rf.currentTerm,
		"from", rf.nodeRole,
		"to", newState)

	// role specific work
	switch newState {
	case Follower:
		// anything?
	case Candidate:
		// anything?
	case Leader:
		go rf.receiveAppendReply()
		rf.setFollowerIndexes()
		rf.replicateLogEntries(true) // send the first heartbeat immediately
	}

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
				rf.logger.Info("election timed out",
					"currentTerm", rf.currentTerm,
					"lastBeat", time.Until(rf.lastBeat),
					"term", rf.currentTerm,
				)
				rf.transition(Candidate)
			}

		case Candidate:
			if rf.needsElection() {
				rf.startElection()
			}

		case Leader:
			rf.replicateLogEntries(false)
		}

		rf.sendCommittedToApp()

		rf.mu.Unlock()
		time.Sleep(stateLoopInterval)
	}
}
