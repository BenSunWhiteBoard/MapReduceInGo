package mr

import (
	"errors"
	"log"
	"os"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

//Master.state
//for this lab, map all before reduce
const (
	Initializing = iota
	MapPhase
	ReducePhase
	TearDown
)

type Master struct {
	// TODO:Your definitions here.
	mutex       sync.Mutex
	state       int
	mapTasks    []Task
	reduceTasks []Task
	numOfMap    int
	numOfReduce int
}

// TODO:Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestTaskHandler(args *RequestTaskArgs, reply *RequestTaskReply) error {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	switch m.state {
	case Initializing:
		reply.WorkerNextState = Idle
	case MapPhase:
		for i, task := range m.mapTasks {
			if task.State == UnScheduled {
				//schedule unassigned task
				task.State = InProgress
				reply = &RequestTaskReply{
					Task:            task,
					WorkerNextState: WorkAssigned,
				}

				m.mapTasks[i].State = InProgress
				m.mapTasks[i].TimeStamp = time.Now()

				return nil
			} else if task.State == InProgress && time.Now().Sub(task.TimeStamp) > 10*time.Second {
				//reassign tasks due to timeout
				reply = &RequestTaskReply{
					Task:            task,
					WorkerNextState: WorkAssigned,
				}
				//update TimeStamp
				m.mapTasks[i].TimeStamp = time.Now()

				return nil
			} else if task.State == Done {
				//ignore the task
				//TODO: array for task is not efficient, maybe change to map?
			}
		}
		//no more mapWork, wait for other tasks
		reply.WorkerNextState = Idle

	case ReducePhase:
		for i, task := range m.reduceTasks {
			if task.State == UnScheduled {
				//schedule unassigned task
				task.State = InProgress
				reply = &RequestTaskReply{
					Task:            task,
					WorkerNextState: WorkAssigned,
				}

				m.reduceTasks[i].State = InProgress
				m.reduceTasks[i].TimeStamp = time.Now()

				return nil
			} else if task.State == InProgress && time.Now().Sub(task.TimeStamp) > 10*time.Second {
				//reassign tasks due to timeout
				reply = &RequestTaskReply{
					Task:            task,
					WorkerNextState: WorkAssigned,
				}
				//update TimeStamp
				m.reduceTasks[i].TimeStamp = time.Now()

			} else if task.State == Done {
				//ignore the task
				//TODO: array for task is not efficient, maybe change to map?
			}
		}
		//no more reduceWork, wait for other tasks
		reply.WorkerNextState = Idle
	default:
		//master gonna be teared down, shut down worker
		//or something weng wrong
		reply.WorkerNextState = NoMoreWork
	}

	return nil
}

func (m *Master) ReportTaskHandler(args *ReportTaskArgs, reply *ReportTaskReply) error {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if args.Task.TaskType == MapTask {
		m.mapTasks[args.Task.Id].State = Done
	} else if args.Task.TaskType == ReduceTask {
		m.reduceTasks[args.Task.Id].State = Done
	}

	switch m.state {
	case MapPhase:
		for _, task := range m.mapTasks {
			if task.State != Done {
				//still has map task left
				return nil
			}
		}
		m.state = ReducePhase
	case ReducePhase:
		for _, task := range m.reduceTasks {
			if task.State != Done {
				//still has reduce task left
				return nil
			}
		}
		m.state = TearDown
	default:
		return errors.New("master is down,worker should be down too")
	}

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

	// TODO:Your code here.

	return m.state == TearDown
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// TODO:Your code here.
	m := Master{
		mutex:       sync.Mutex{},
		state:       Initializing,
		mapTasks:    []Task{},
		reduceTasks: []Task{},
		numOfMap:    len(files),
		numOfReduce: nReduce,
	}

	for i, file := range files {
		m.mapTasks = append(m.mapTasks, Task{
			Id:           i,
			TaskType:     MapTask,
			State:        UnScheduled,
			Filename:     file,
			NumsOfMap:    m.numOfMap,
			NumsOfReduce: m.numOfReduce,
		})
	}

	for i := 0; i < nReduce; i ++ {
		m.mapTasks = append(m.mapTasks, Task{
			Id:           i,
			TaskType:     ReduceTask,
			State:        UnScheduled,
			Filename:     "",
			NumsOfMap:    m.numOfMap,
			NumsOfReduce: m.numOfReduce,
		})
	}

	m.state = MapPhase

	m.server()
	return &m
}
