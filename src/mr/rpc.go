package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
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

//
// Add your RPC definitions here.

//Task.TaskType
const(
	MapTask = iota
	ReduceTask
)
//Task.State
const (
	UnScheduled = iota
	InProgress
	Done
)
//master maintain the full consistent info of Task, worker only need part of it to work
type Task struct {
	Id int
	TaskType int
	State int
	Filename string
	TimeStamp time.Time
	NumsOfMap int
	NumsOfReduce int
}

//RequestTaskReply.WorkerNextState
const (
	Idle = iota
	WorkAssigned
	NoMoreWork
)

// args and reply for CallRequestTask
type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	Task Task
	WorkerNextState int
}

// args and reply for  CallReportTask RPC
type ReportTaskArgs struct {
	Task Task
}

type ReportTaskReply struct {
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
