package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sync"
	"encoding/json"
	"time"
)

// Map functions return a slice of KeyValue.

type KeyValue struct {
	Key   string
	Value string
}

// static variable to store worker id

var workerIdLock sync.Mutex
var workerIDCounter int = 0

func getUniqueWorkerId() int {
	workerIdLock.Lock()
	defer workerIdLock.Unlock()

	workerIDCounter++
	return workerIDCounter
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerId := getUniqueWorkerId()
	for {
		task := RequestTask(workerId)
		if task.TaskType == "Wait" {
			time.Sleep(1 * time.Second)
			continue
		} else if task.TaskType == "Done" {
			break
		}
		ExecuteTask(task, mapf, reducef)
		CompleteTask(task)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func RequestTask(workerId int) TaskResponse {
	args := TaskRequest{WorkerId: workerId}
	reply := TaskResponse{}
	ok := call("Coordinator.Request", &args, &reply)

	if ok {
		// fmt.Printf("reply %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}

func ExecuteTask(task TaskResponse, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	if task.TaskType == "Map" {
		// Map task: process the file and write intermediate results to mr-X-Y files
		filename := task.TaskFile
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()

		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}

		// Apply the map function to the file content
		kva := mapf(filename, string(content))

		// Write intermediate results to mr-X-Y files
		tempFiles := make(map[string]string)

		for i := range task.NReduce {
			oname := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
			tempFile, err := os.CreateTemp(".", "mr-XXXXXX")
			if err != nil {
				log.Fatalf("cannot create temporary file: %v", err)
			}
			tempFiles[oname] = tempFile.Name()
			defer os.Remove(tempFile.Name()) // Ensure temporary file is removed if something goes wrong
			defer tempFile.Close()
		}

		for _, kv := range kva {
			reduceId := ihash(kv.Key) % task.NReduce
			oname := fmt.Sprintf("mr-%d-%d", task.TaskId, reduceId)
			tempFileName := tempFiles[oname]
			ofile, err := os.OpenFile(tempFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
			if err != nil {
				log.Fatalf("cannot open/create %v", tempFileName)
			}
			defer ofile.Close()

			enc := json.NewEncoder(ofile)
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}

		for oname, tempFileName := range tempFiles {
			// Rename the temporary file to the final output file
			err := os.Rename(tempFileName, oname)
			if err != nil {
				log.Fatalf("cannot rename %v to %v", tempFileName, oname)
			}
		}
	} else if task.TaskType == "Reduce" {
		// Reduce task: read all intermediate files for this reduce task and apply reduce function
		intermediate := make(map[string][]string)
	
		// Find all intermediate files for this reduce task
		files, err := os.ReadDir(".")
		if err != nil {
			log.Fatalf("cannot read directory: %v", err)
		}
	
		// Filter files that match the pattern mr-%d-%d
		var reduceId int
		for _, file := range files {
			if file.IsDir() {
				continue
			}
			fileName := file.Name()
			var mapId int
			// Check if the file name matches the pattern mr-%d-%d
			if _, err := fmt.Sscanf(fileName, "mr-%d-%d", &mapId, &reduceId); err == nil {
				// Only process files that match the reduce task id
				if reduceId != task.TaskId {
					continue
				}

				oname := fileName
				file, err := os.Open(oname)
				if err != nil {
					log.Fatalf("cannot open %v", oname)
				}
				defer file.Close()
	
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						if err == io.EOF {
							break
						}
						log.Fatalf("cannot decode %v", err)
					}
					intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
				}
			}
		}

		tempFile, err := os.CreateTemp(".", "mr-XXXXXX")
		if err != nil {
			log.Fatalf("cannot create temporary file: %v", err)
		}
		defer os.Remove(tempFile.Name()) // Ensure temporary file is removed if something goes wrong
		defer tempFile.Close()
	
		// Apply the reduce function and write the output to a file
		for key, values := range intermediate {
			output := reducef(key, values)
			fmt.Fprintf(tempFile, "%v %v\n", key, output)
		}

		oname := fmt.Sprintf("mr-out-%d", task.TaskId)
		err = os.Rename(tempFile.Name(), oname)
		if err != nil {
			log.Fatalf("cannot rename %v to %v", tempFile.Name(), oname)
		}
	}
}

func CompleteTask(task TaskResponse) {
	args := TaskCompletion{TaskId: task.TaskId, TaskType: task.TaskType}
	reply := TaskCompletionResponse{}
	call("Coordinator.Complete", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
