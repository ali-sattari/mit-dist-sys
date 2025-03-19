package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type WorkerStatus int

const (
	WorkerIdle WorkerStatus = iota
	WorkerBusy
	WorkerDead
)

type WorkerRecord struct {
	id       int64
	lastSeen time.Time
	status   WorkerStatus
}

type Coordinator struct {
	workers map[int64]WorkerRecord
	counter int64
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 10
	return nil
}

func (c *Coordinator) Ping(args *PingRequest, reply *PingResponse) error {
	c.processPing(args.ID)

	// check if there are still tasks to be done

	if c.counter >= 10 {
		reply.Type = "exit"
	} else {
		reply.Type = "pong"
	}
	c.counter += 1

	return nil
}

func (c *Coordinator) processPing(id int64) {
	w, ok := c.workers[id]
	if ok {
		w.lastSeen = time.Now()
	} else {
		c.workers[id] = WorkerRecord{
			id:       id,
			lastSeen: time.Now(),
		}
	}
	// fmt.Printf("Got ping from worker %d: %+v\n", id, c.workers[id])
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		workers: map[int64]WorkerRecord{},
	}

	// Your code here.

	c.server()
	return &c
}
