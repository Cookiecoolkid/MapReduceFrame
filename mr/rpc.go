package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type TaskRequest struct {
	WorkerId int
}

type TaskResponse struct {
	TaskId int
	TaskType string
	TaskFile string

	// Number of reduce tasks
	NReduce int
}

type TaskCompletion struct {
	TaskId int
	TaskType string
}

type TaskCompletionResponse struct {
	Success bool
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
