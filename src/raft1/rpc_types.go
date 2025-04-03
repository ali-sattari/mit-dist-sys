package raft

type AppendEntryArgs struct {
	Term         uint
	LeaderId     uint
	PrevLogIndex uint
	PrevLogTerm  uint
	LeaderCommit uint
	Entries      []LogEntry
}

type AppendEntryReply struct {
	Term    uint
	Success bool
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         uint
	CandidateId  uint
	LastLogIndex uint
	LastLogTerm  uint
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        uint
	VoteGranted bool
}
