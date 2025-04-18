package raft

import "fmt"

type AppendEntryArgs struct {
	Term         uint
	LeaderId     int
	PrevLogIndex uint
	PrevLogTerm  uint
	LeaderCommit uint
	Entries      []LogEntry
}

type AppendEntryReply struct {
	Term    uint
	Success bool
}

type AppendEntryResult struct {
	Server   int
	Entries  []LogEntry
	Response AppendEntryReply
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         uint
	CandidateId  int
	LastLogIndex uint
	LastLogTerm  uint
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        uint
	VoteGranted bool
}

type LogEntry struct {
	Id      uint
	Term    uint
	Command any
}

func (l LogEntry) String() string {
	var c string
	if l.Command != nil {
		c = truncateWithEllipsis(l.Command, 11)
	}
	return fmt.Sprintf(
		"Log{Id:%d, Term:%d, Cmd:%s}",
		l.Id,
		l.Term,
		c,
	)
}

func (x AppendEntryResult) String() string {
	return fmt.Sprintf(
		"AppendResult{Server:%d, Res:%+v, Logs:%+v}",
		x.Server,
		x.Response,
		x.Entries,
	)
}

// TODO: string for all types
