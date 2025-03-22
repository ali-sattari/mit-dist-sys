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

	"github.com/gammazero/deque"
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
	queue      deque.Deque[TaskId]
	todo       map[TaskId]*Task
	done       map[TaskId]*Task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Ping(args *PingRequest, reply *PingResponse) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.processPing(args.ID)

	// log.Printf("Ping: workers %d, todo %d, done %d, queue %d", len(c.workers), len(c.todo), len(c.done), c.queue.Len())

	// check if there are still tasks to be done
	if len(c.todo) == 0 {
		reply.Type = "exit"
	} else {
		reply.Type = "pong"
	}

	return nil
}

func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	w := c.updateWorkerStatus(args.ID)

	if c.queue.Len() > 0 {
		// find the first unassigned task
		first := c.queue.Front()
		if c.todo[first].step != "map" && !c.areMapTasksDone() {
			log.Printf("map tasks are not done %+v\n", c.todo[first])
			return fmt.Errorf("map tasks are not done")
		}

		next := c.queue.PopFront()
		t := c.todo[next]

		reply.Id = t.id
		reply.Step = t.step
		reply.File = t.file
		reply.ReduceIndex = t.parition
		reply.ReduceCount = c.nReduce
		reply.FileCount = len(c.files)

		t.assignedAt = time.Now()
		t.worker = w

		// log.Printf("GetTask: for worker %d got %+v: %+v\n", args.ID, next, c.todo[next])

	}

	return nil
}

func (c *Coordinator) UpdateTask(args *UpdateTaskRequest, reply *UpdateTaskResponse) error {
	// TODO: check for result (success/failure) and mark repeatedly failing tasks

	c.mtx.Lock()
	defer c.mtx.Unlock()

	t, ok := c.todo[args.TaskId]
	if ok && t.worker != nil && t.worker.id == args.WorkerId {
		// log.Printf("UpdateTask: remove %d from todo and add to done", args.TaskId)
		c.done[args.TaskId] = t
		delete(c.todo, args.TaskId)
	} else {
		log.Printf("UpdateTask: task %d not found or not assigned to worker %d in todo!", args.TaskId, args.WorkerId)
	}

	reply.Okay = true
	return nil
}

func (c *Coordinator) areMapTasksDone() bool {
	done := true
	for _, t := range c.todo {
		if t.step == "map" {
			done = false
		}
	}

	return done
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

func (c *Coordinator) checkTaskStatus() {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	deadline := time.Second
	lastVaild := time.Now().Add(-deadline)
	for id, task := range c.todo {
		if task.worker != nil && task.assignedAt.Before(lastVaild) {
			// log.Printf("task %d taking too long (%+v) to be processed by %+v, re-queuing", id, time.Until(task.assignedAt), task.worker)
			if task.step == "map" {
				c.queue.PushFront(id)
			} else {
				c.queue.PushBack(id)
			}
			c.todo[id].worker = nil
		}
	}
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

	// add tasks for map and reduce to the queue
	for _, f := range files {
		c.todo[c.lastTaskId] = &Task{
			id:   c.lastTaskId,
			step: "map",
			file: f,
		}
		c.queue.PushBack(c.lastTaskId)
		c.lastTaskId++
	}

	for n := range nReduce {
		c.todo[c.lastTaskId] = &Task{
			id:       c.lastTaskId,
			step:     "reduce",
			parition: n,
		}
		c.queue.PushBack(c.lastTaskId)
		c.lastTaskId++
	}

	// loop to check task status and re-assign
	cheker := periodicJobs(time.Millisecond*500, c.checkTaskStatus)
	go cheker()

	c.server()
	return &c
}
