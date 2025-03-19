package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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
	// loop to ping
	pinger := periodicJobs(time.Millisecond*500, func() {
		if Ping(0) {
			sig <- true
		}
	})
	go pinger()

	// CallExample()
	// loop to poll coordinator for tasks
	// RPC call to coordinator to get task
	// execute map or reduce func based on task
	// split map files?

	// catch exit signal from chan
	switch {
	case <-sig:
		// wait for something?
		os.Exit(0)
	}

}

func Ping(id int64) bool {
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

func GetTask(id int64) {

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
