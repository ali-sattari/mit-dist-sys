package mr

import (
	"bufio"
	"encoding/json"
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
			switch t.Step {
			case "map":
				content, err := os.ReadFile(t.File)
				if err != nil {
					log.Printf("Error reading file %s: %v\n", t.File, err)
					return
				}
				writeMapResults(
					t.MapIndex,
					t.ReduceCount,
					mapf(t.File, string(content)),
				)

				// register success with coordinator
				updateTask(t.Id)

			case "reduce":
				rInp := getReduceInput(t)
				for k, vl := range rInp {
					err := writeReduceResults(t.ReduceCount, k, reducef(k, vl))
					if err != nil {
						log.Printf("Error writing results file %s: %v\n", t.File, err)
					}
				}

				// register success with coordinator
				updateTask(t.Id)

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

func getTask(id int) *GetTaskResponse {
	resp := GetTaskResponse{}
	ok := call("Coordinator.GetTask", &GetTaskRequest{ID: id}, &resp)

	if ok {
		// fmt.Printf("GetTask reply %+v\n", resp)
		return &resp
	}

	return nil
}

func updateTask(id int) *UpdateTaskResponse {
	resp := UpdateTaskResponse{}
	ok := call("Coordinator.UpdateTask", &UpdateTaskRequest{Id: id}, &resp)

	if ok {
		// fmt.Printf("UpdateTask reply %+v\n", resp)
		return &resp
	}

	return nil
}

func getReduceInput(t *GetTaskResponse) map[string][]string {
	res := map[string][]string{}
	f := getInterimFileName(t.MapIndex, t.ReduceIndex)

	file, err := os.Open(f)
	if err != nil {
		log.Printf("getReduceInput: error opening file %s: %v\n", f, err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	for {
		var x map[string][]string
		if err := dec.Decode(&x); err != nil {
			// log.Printf("Error decoding json %s: %v\n", f, err)
			break
		}
		for k, vs := range x {
			res[k] = vs
		}
	}

	return res
}

func writeMapResults(fileId int, buckets int, content []KeyValue) {
	sort.Slice(content, func(i, j int) bool {
		return content[i].Key <= content[j].Key
	})

	last_hash := 0
	last_index := 0
	for i := range content {
		h := ihash(content[i].Key)
		if last_hash != h {
			f := getInterimFileName(fileId, last_hash%buckets)
			err := writeMapToFile(f, content[last_index:i])
			if err != nil {
				log.Printf("error writing intermediary results for %s: %+v", f, err)
			}
			last_hash = h
			last_index = i
		}
	}
}

func writeMapToFile(path string, content []KeyValue) error {
	if len(content) == 0 {
		// log.Printf("writeMapToFile: empty content!")
		return nil
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := json.NewEncoder(file)

	data := map[string][]string{}
	data[content[0].Key] = []string{}
	for i := range content {
		data[content[0].Key] = append(data[content[0].Key], content[i].Value)
	}

	return enc.Encode(&data)
}

func writeReduceResults(buckets int, key, content string) error {
	f := getFinalFileName(ihash(key) % buckets)
	file, err := os.OpenFile(f, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("writeReduceResults: error opening file for %s: %+v", f, err)
	}
	defer file.Close()

	// log.Printf("reduce result for %s: %s %s\n", f, key, content)

	writer := bufio.NewWriter(file)
	_, err = writer.WriteString(fmt.Sprintf("%v %v\n", key, content))
	if err != nil {
		log.Printf("error writing final results for %s: %+v", f, err)
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
