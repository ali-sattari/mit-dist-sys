package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	sig := make(chan bool)
	wid := os.Getpid()
	// loop to ping
	pinger := periodicJobs(time.Millisecond*500, func() {
		if ping(wid) {
			sig <- true
		}
	})
	go pinger()

	// loop to poll coordinator for tasks
	busy := false
	tasker := periodicJobs(time.Second, func() {
		if busy {
			return
		}

		t := getTask(wid)
		if t != nil {
			busy = true
			switch t.Type {
			case "map":
				content, err := os.ReadFile(t.File)
				if err != nil {
					log.Printf("Error reading file: %v\n", err)
					return
				}
				writeResults(t.File, t.Count, mapf(t.File, string(content)))

				// register success with coordinator

			case "reduce":

			}
			busy = false
		}
	})
	go tasker()

	// catch exit signal from chan
	switch {
	case <-sig:
		// wait for something?
		os.Exit(0)
	}
}

func ping(id int) bool {
	resp := PingResponse{}
	ok := call("Coordinator.Ping", &PingRequest{ID: id}, &resp)

	if ok {
		// fmt.Printf("Ping reply %+v\n", resp)
		if resp.Type == "exit" {
			return true
		}
	} else {
		fmt.Printf("Ping call failed!\n")
	}

	return false
}

func getTask(id int) *TaskResponse {
	resp := TaskResponse{}
	ok := call("Coordinator.GetTask", &TaskRequest{ID: id}, &resp)

	if ok {
		// fmt.Printf("GetTask reply %+v\n", resp)
		return &resp
	}

	return nil
}

func writeResults(file string, buckets int, content []KeyValue) {
	sort.Slice(content, func(i, j int) bool {
		return content[i].Key >= content[j].Key
	})

	last_hash := 0
	last_index := 0
	for i := range content {
		h := ihash(content[i].Key)
		if last_hash != h {
			f := getInterimFileName(ihash(file)%buckets, last_hash%buckets)
			err := writeToFile(f, content[last_index:i])
			if err != nil {
				log.Printf("error writing intermediary results for %s: %+v", f, err)
			}
			last_hash = h
			last_index = i
		}
	}
}

func writeToFile(path string, content []KeyValue) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for i := range content {
		_, err := writer.WriteString(fmt.Sprintf("%v %v\n", content[i].Key, content[i].Value))
		if err != nil {
			return err
		}
	}
	return writer.Flush()
}

func getInterimFileName(m, r int) string {
	return fmt.Sprintf("mr-%d-%d", m, r)
}

func getFinalFileName(n int) string {
	return fmt.Sprintf("mr-out-%d", n)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err, rpcname, args)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
