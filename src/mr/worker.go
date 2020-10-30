package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func DoMapTask(mapf func(string, string) []KeyValue, task Task) {

	file, err := os.Open(task.Filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}

	kva := mapf(task.Filename, string(content))
	//divide into nReduce groups
	reduceTasks := make(map[int][]KeyValue, task.NumsOfReduce)
	for _, kv := range kva {
		reduceTaskNumber := ihash(kv.Key) % task.NumsOfReduce
		reduceTasks[reduceTaskNumber] = append(reduceTasks[reduceTaskNumber], kv)
	}

	//partition into nReduce kvs and generate corresponding intermediate files
	for reduceTaskId := 0; reduceTaskId < task.NumsOfReduce; reduceTaskId++ {
		generateIntermediate(task.Id, reduceTaskId, reduceTasks[reduceTaskId])
	}

}

func generateIntermediate(mapTaskId int, reduceTaskId int, kvs []KeyValue) {
	intermediateFileName := IntermediateFileName(mapTaskId, reduceTaskId)
	intermediateFile, err := os.Create(intermediateFileName)
	defer intermediateFile.Close()
	if err != nil {
		return
	}

	enc := json.NewEncoder(intermediateFile)
	for _, kv := range kvs {
		enc.Encode(&kv)
	}

	intermediateFile.Close()
}

func IntermediateFileName(mapTaskId int, reduceTaskId int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskId, reduceTaskId)
}

func mergeIntermediate(mapTaskId int, reduceTaskId int, kvMap map[string][]string) {
	intermediateFileName := IntermediateFileName(mapTaskId, reduceTaskId)
	intermediateFile, err := os.Open(intermediateFileName)
	defer intermediateFile.Close()
	if err != nil {
		return
	}

	dec := json.NewDecoder(intermediateFile)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
	}
}

func DoReduceTask(reducef func(string, []string) string, task Task) {

	kvMap := map[string][]string{}
	//merge kvs in X intermediate files(mr-X-Y) into kvMap
	//Shuffle
	for MapTaskId := 0; MapTaskId < task.NumsOfMap; MapTaskId++ {
		mergeIntermediate(MapTaskId, task.Id, kvMap)
	}
	//Sort
	var keys []string
	for key, _ := range kvMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	//generate output file
	outputFile, err := os.Create("mr-out-" + strconv.Itoa(task.Id))
	defer outputFile.Close()
	if err != nil {
		return
	}
	//reduce
	for _, key := range keys {
		countString := reducef(key, kvMap[key])
		outputFile.WriteString(fmt.Sprintf("%v %v\n", key, countString))
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// keep asking for tasks
	for {
		reply, err := CallRequestTask()
		if err != nil {
			continue
		}
		switch reply.WorkerNextState {
		case Idle:
			time.Sleep(time.Second)
		case WorkAssigned:
			if reply.Task.TaskType == MapTask {
				DoMapTask(mapf, reply.Task)
			} else if reply.Task.TaskType == ReduceTask {
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

//call for Master.RequestTask
func CallRequestTask() (RequestTaskReply, error) {

	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	isSuccess := call("Master.RequestTask", &args, &reply)
	if isSuccess {
		return reply, nil
	} else {
		//something go wrong
		return RequestTaskReply{}, errors.New("failed to connect to master server")
	}
}

//call for Master.ReportTask
func CallReportTask(task Task) error {

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
