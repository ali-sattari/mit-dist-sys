package raft

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
	server   int
	entries  []LogEntry
	response AppendEntryReply
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
