package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskInfo struct {
	TaskType   string
	TaskFile   string // Task file name
	WorkerID   int
	StartTime  time.Time
	Status     string // Task status: "Pending", "InProgress", "Completed"
}

type Coordinator struct {
	NReduce              int
	TaskStatus           map[int]TaskInfo
	mu                   sync.Mutex
	MapTaskIDs           []int
	ReduceTaskIDs        []int
}

// 检查所有 Map 任务是否完成
func (c *Coordinator) areMapTasksCompleted() bool {
	for _, taskID := range c.MapTaskIDs {
		taskInfo := c.TaskStatus[taskID+c.NReduce] // Map tasks are stored with offset NReduce
		if taskInfo.Status != "Completed" {
			return false
		}
	}
	return true
}

// 检查所有 Reduce 任务是否完成
func (c *Coordinator) areReduceTasksCompleted() bool {
	for _, taskID := range c.ReduceTaskIDs {
		taskInfo := c.TaskStatus[taskID] // Reduce tasks are stored without offset
		if taskInfo.Status != "Completed" {
			return false
		}
	}
	return true
}

// RPC handlers for the worker to call.

func (c *Coordinator) Request(args *TaskRequest, reply *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Assign Map task
	for _, taskID := range c.MapTaskIDs {
		taskInfo := c.TaskStatus[taskID+c.NReduce] // Map tasks are stored with offset NReduce
		if taskInfo.Status == "Pending" {
			taskInfo.WorkerID = args.WorkerId
			taskInfo.StartTime = time.Now()
			taskInfo.Status = "InProgress"

			c.TaskStatus[taskID+c.NReduce] = taskInfo

			reply.TaskType = "Map"
			reply.TaskId = taskID
			reply.TaskFile = taskInfo.TaskFile
			reply.NReduce = c.NReduce
			return nil
		}
	}

	mapTasksCompleted := c.areMapTasksCompleted()
	reduceTasksCompleted := c.areReduceTasksCompleted()

	if mapTasksCompleted && reduceTasksCompleted {
		reply.TaskType = "Done"
		return nil
	}

	if mapTasksCompleted {
	// Assign Reduce task
		for _, taskID := range c.ReduceTaskIDs {
			taskInfo := c.TaskStatus[taskID] // Reduce tasks are stored without offset
			if taskInfo.Status == "Pending" {
				taskInfo.WorkerID = args.WorkerId
				taskInfo.StartTime = time.Now()
				taskInfo.Status = "InProgress"
				c.TaskStatus[taskID] = taskInfo

				reply.TaskType = "Reduce"
				reply.TaskId = taskID
				reply.TaskFile = ""
				reply.NReduce = c.NReduce
				return nil
			}
		}
	} 

	reply.TaskType = "Wait"
	return nil
}

func (c *Coordinator) Complete(args *TaskCompletion, reply *TaskCompletionResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskID := args.TaskId
	var taskInfo TaskInfo

	if args.TaskType == "Map" {
		taskInfo = c.TaskStatus[taskID+c.NReduce] // Map tasks are stored with offset NReduce
	} else if args.TaskType == "Reduce" {
		taskInfo = c.TaskStatus[taskID] // Reduce tasks are stored without offset
	} else {
		log.Fatalf("Invalid task type: %s", args.TaskType)
	}

	if taskInfo.Status == "InProgress" {
		taskInfo.Status = "Completed"
		if args.TaskType == "Map" {
			c.TaskStatus[taskID+c.NReduce] = taskInfo
		} else {
			c.TaskStatus[taskID] = taskInfo
		}
	}

	return nil
}

func (c *Coordinator) monitorTasks() {
	ticker := time.NewTicker(10 * time.Second) // check every 10 seconds
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for taskID, info := range c.TaskStatus {
			// timeout 10s
			if info.Status == "InProgress" && now.Sub(info.StartTime) > 10*time.Second {
				// re-assigned to another worker
				info.Status = "Pending"
				c.TaskStatus[taskID] = info
			}
		}
		c.mu.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
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
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.areMapTasksCompleted() && c.areReduceTasksCompleted()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:         nReduce,
		TaskStatus:      make(map[int]TaskInfo),
		MapTaskIDs:      []int{},
		ReduceTaskIDs:   []int{},
	}

	// Initialize Map tasks
	for i, file := range files {
		taskID := i
		c.TaskStatus[taskID+nReduce] = TaskInfo{ // Map tasks are stored with offset NReduce
			TaskType: "Map",
			TaskFile: file,
			Status:   "Pending",
		}
		c.MapTaskIDs = append(c.MapTaskIDs, taskID)
	}

	// Initialize Reduce tasks
	for i := 0; i < nReduce; i++ {
		taskID := i
		c.TaskStatus[taskID] = TaskInfo{ // Reduce tasks are stored without offset
			TaskType: "Reduce",
			Status:   "Pending",
		}
		c.ReduceTaskIDs = append(c.ReduceTaskIDs, taskID)
	}

	go c.monitorTasks()

	c.server()
	return &c
}