package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type PingRequest struct {
	ID int // worker id
}
type PingResponse struct {
	Type string // pong, exit
}

type GetTaskRequest = PingRequest
type GetTaskResponse struct {
	Step        string // map, reduce
	Id          int    // task id
	File        string // file path
	ReduceCount int    // reduce output count
	ReduceIndex int    // reduce input index
	FileCount   int
}

type UpdateTaskRequest struct {
	TaskId   int
	WorkerId int
	Result   string // success, failure
}
type UpdateTaskResponse struct {
	Okay bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
