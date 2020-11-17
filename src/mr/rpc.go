package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskWorker struct {
	FileName string
	TaskNo int
	NReduce int
	IsMapTask bool
	IsReduceTask bool
	ReceiveTimestamp int64
}

func (task *TaskWorker) equals(task1 *TaskWorker) bool {
	sameFile := task1.FileName == task.FileName
	sameNo := task1.TaskNo == task.TaskNo
	sameType := task1.IsMapTask == task.IsMapTask
	return sameFile && sameNo && sameType
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
