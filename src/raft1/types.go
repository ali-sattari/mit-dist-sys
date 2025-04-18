package raft

import "fmt"

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

type AppendEntryResult struct {
	Server   int
	Entries  []LogEntry
	Response AppendEntryReply
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Id      int
	Term    int
	Command any
}

// STring methods for nice logs
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

func (a AppendEntryArgs) String() string {
	return fmt.Sprintf(
		"AppendEntryArgs{Term:%d, LeaderId:%d, PrevLogIdx:%d, PrevLogTerm:%d, LeaderCommit:%d, Entries:%+v}",
		a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit, a.Entries,
	)
}

func (r AppendEntryReply) String() string {
	return fmt.Sprintf("AppendEntryReply{Term:%d, Success:%t}", r.Term, r.Success)
}

func (x AppendEntryResult) String() string {
	return fmt.Sprintf(
		"AppendEntryResult{Server:%d, Res:%+v, Logs:%+v}",
		x.Server, x.Response, x.Entries,
	)
}

func (a RequestVoteArgs) String() string {
	return fmt.Sprintf(
		"RequestVoteArgs{Term:%d, CandidateId:%d, LastLogIdx:%d, LastLogTerm:%d}",
		a.Term, a.CandidateId, a.LastLogIndex, a.LastLogTerm,
	)
}

func (r RequestVoteReply) String() string {
	return fmt.Sprintf("RequestVoteReply{Term:%d, VoteGranted:%t}", r.Term, r.VoteGranted)
}
