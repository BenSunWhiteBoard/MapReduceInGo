package mr

import (
	"errors"
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {

}

func IntermediateFile(mapTaskId int, reduceTask int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskId, reduceTask)
}

func DoReduceTask(reducef func(string, []string) string, task *Task) {

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// TODO:Your worker implementation here.
	// asking for tasks
	for {
		reply, err := CallRequestTask()
		if err != nil {
			continue
		}
		switch reply.WorkerNextState {
		case Idle:
			time.Sleep(time.Second)
		case WorkAssigned:
			switch reply.Task.taskType {
			case MapTask:
				DoMapTask(mapf, reply.Task)
			case ReduceTask:
				DoReduceTask(reducef, reply.Task)
			}
			CallReportTask(reply.Task)
			time.Sleep(time.Second)
		case NoMoreWork:
			return
		default:
			panic("Something wrong with reply")
		}

	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func CallRequestTask() (RequestTaskReply, error) {

	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	isSuccess := call("Master.RequestTask", &args, &reply)
	if isSuccess {
		return reply, nil
	}
	//something go wrong
	return RequestTaskReply{}, errors.New("failed to connect to master server")
}

func CallReportTask(task *Task) error {

	args := ReportTaskArgs{task}
	reply := RequestTaskReply{}

	isSuccess := call("Master.ReportTask", &args, &reply)
	if isSuccess {
		return nil
	} else {
		return errors.New("failed to report task status back to master server")
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	//c, err := rpc.DialHTTP("tcp", "localhost:8080" )
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
