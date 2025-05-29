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
	XTerm   int // Term of conflicting entry
	XIndex  int // First index of XTerm
	XLen    int // Follower's log length
}

type AppendEntryResult struct {
	PeerId  int
	Entries []LogEntry
	AppendEntryReply
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type RequestVoteResult struct {
	PeerId int
	RequestVoteReply
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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
		a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit, len(a.Entries),
	)
}

func (r AppendEntryReply) String() string {
	return fmt.Sprintf("AppendEntryReply{Term:%d, Success:%t, XTerm:%d, XIndex:%d, XLen:%d}", r.Term, r.Success, r.XTerm, r.XIndex, r.XLen)
}

func (x AppendEntryResult) String() string {
	return fmt.Sprintf(
		"AppendEntryResult{PeerID:%d, Rep:%+v, Logs:%+v}",
		x.PeerId, x.AppendEntryReply, len(x.Entries),
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

func (x RequestVoteResult) String() string {
	return fmt.Sprintf(
		"RequestVoteResult{PeerID:%d, Rep:%+v}",
		x.PeerId, x.RequestVoteReply,
	)
}

func (a InstallSnapshotArgs) String() string {
	return fmt.Sprintf(
		"InstallSnapshotArgs{Term:%d, LeaderId:%d, LastIncludedIndex:%d, LastIncludedTerm:%d}",
		a.Term, a.LeaderId, a.LastIncludedIndex, a.LastIncludedTerm,
	)
}

func (r InstallSnapshotReply) String() string {
	return fmt.Sprintf("InstallSnapshotReply{Term:%d}", r.Term)
}
