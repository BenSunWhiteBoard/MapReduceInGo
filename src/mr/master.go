package mr

import (
	"log"
	"os"
	"sync"
)
import "net"
import "net/rpc"
import "net/http"


type Master struct {
	// TODO:Your definitions here.
	mutex sync.Mutex
	state int
	mapTasks []Task
	reduceTasks []Task
	numOfMap int
	numOfReduce int
}

// TODO:Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestTaskHandler(args *RequestTaskArgs, reply *RequestTaskReply) error {


	return nil
}

func (m *Master) ReportTaskHandler(args *RequestTaskArgs, reply *RequestTaskReply) error {


	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", "localhost:8080")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := true

	// TODO:Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// TODO:Your code here.
	for i := range(nReduce) {

	}

	m.server()
	return &m
}
