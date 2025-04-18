# MIT Distributed Systems course assignments

<https://pdos.csail.mit.edu/6.824/schedule.html>

## Lab 1: Map Reduce

* Two programs: coordinator, worker
* They communicate using RPC
* Workers ask for tasks from coordinator
  * Upon receiving they execute tasks over input file(s) and save result to output file(s)
  * Once finished workers ask for another task
* Coordinator keeps track of which task was given to which worker
  * Coordinator waits for a timeout (10s) and gives the task to another worker
  * Coordinator should know when all tasks are done
  * Workers should also get a signal that tasks are done and exit
* Files are already split (pg-*.txt) and are passed to map tasks
* Mappers should output to nReduce files
* Coordinator should wait 10s before re-assigning tasks to a new worker

## Lab 2: Key/Value Service

* A key/value server with GET and PUT operations
* On single machine
* Each Put operation is executed at-most-once despite network failures (via version checks)
* Operations should be linearizable
* Retry mechanism in KV client

## Lab 3: Raft

Very useful [blog post](https://thesquareplanet.com/blog/students-guide-to-raft/) for common pitfalls.

### 3A: Leader Election

* Need and internal loop for node state management
* Heartbeat through light-weight AppendEntry RPC
* Each term can have at most one leader -> why candidates increment term at start of new election round

### 3B: Log

* Send each committed entry to applyChan for the application layer
  * A log entry is committed once the leader that created the entry has replicated it on a majority of the servers
* Start log at index 0, because easy

* flappy tests:
  * TestFailNoAgree3B
  * TestBackup3B
