package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type WorkerStatus int

const (
	WorkerIdle WorkerStatus = iota
	WorkerBusy
	WorkerDead
)

type WorkerRecord struct {
	id       int
	lastSeen time.Time
	status   WorkerStatus
}

type Task struct {
	step       string
	id         int
	file       string
	mapId      int
	parition   int
	worker     *WorkerRecord
	assignedAt time.Time
}

type TaskId = int
type Coordinator struct {
	files      []string
	workers    map[int]*WorkerRecord
	nReduce    int
	mtx        *sync.RWMutex
	lastTaskId TaskId
	todo       map[TaskId]*Task
	done       map[TaskId]*Task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Ping(args *PingRequest, reply *PingResponse) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.processPing(args.ID)

	log.Printf("Ping: workers %d, todo %d, done %d", len(c.workers), len(c.todo), len(c.done))

	// check if there are still tasks to be done
	if len(c.todo) == 0 {
		reply.Type = "exit"
	} else {
		reply.Type = "pong"
	}

	return nil
}

func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	w := c.updateWorkerStatus(args.ID)

	if len(c.todo) > 0 {
		// find the first unassigned task
		for id, t := range c.todo {
			// TODO: check for assignedAt field if worker is not nil and reassign it if more than X seconds
			if t.worker == nil {
				reply.Id = t.id
				reply.Step = t.step
				reply.File = t.file
				reply.MapIndex = t.mapId
				reply.ReduceIndex = t.parition
				reply.ReduceCount = c.nReduce
				reply.FileCount = len(c.files)

				t.assignedAt = time.Now()
				t.worker = w

				log.Printf("GetTask: for worker %d got %+v: %+v\n", args.ID, id, c.todo[id])
				break
			}
		}
	}

	return nil
}

func (c *Coordinator) UpdateTask(args *UpdateTaskRequest, reply *UpdateTaskResponse) error {
	// TODO: check for result (success/failure) and mark repeatedly failing tasks

	c.mtx.Lock()
	defer c.mtx.Unlock()

	t, ok := c.todo[args.Id]
	if ok {
		if t.step == "map" {
			// move to reduce
			for n := range c.nReduce {
				c.todo[c.lastTaskId] = &Task{
					id:       c.lastTaskId,
					step:     "reduce",
					mapId:    t.id,
					parition: n,
					file:     t.file,
				}
				c.lastTaskId++
			}
		}

		// log.Printf("UpdateTask: remove %d from todo and add to done", args.Id)
		c.done[args.Id] = t
		delete(c.todo, args.Id)
	} else {
		log.Printf("UpdateTask: task %d not found in todo!", args.Id)
	}

	reply.Okay = true
	return nil
}

func (c *Coordinator) processPing(id int) {
	w, ok := c.workers[id]
	if ok {
		w.lastSeen = time.Now()
	} else {
		c.workers[id] = &WorkerRecord{
			id:       id,
			lastSeen: time.Now(),
		}
	}
	// fmt.Printf("Got ping from worker %d: %+v\n", id, c.workers[id])
}

func (c *Coordinator) updateWorkerStatus(id int) *WorkerRecord {
	w, ok := c.workers[id]
	if ok {
		w.lastSeen = time.Now()
		w.status = WorkerBusy
	} else {
		c.workers[id] = &WorkerRecord{
			id:       id,
			lastSeen: time.Now(),
			status:   WorkerBusy,
		}
	}

	return w
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	// log.Printf("server socket: %+v\n", sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	if len(c.todo) == 0 {
		return true
	}

	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		files:   files,
		mtx:     &sync.RWMutex{},
		workers: map[int]*WorkerRecord{},
		todo:    map[TaskId]*Task{},
		done:    map[TaskId]*Task{},
	}

	// Your code here.
	fmt.Printf("new coordinator with files: %+v\n", files)
	for _, f := range files {
		c.todo[c.lastTaskId] = &Task{
			id:   c.lastTaskId,
			step: "map",
			file: f,
		}
		c.lastTaskId++
	}

	c.server()
	return &c
}
