package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	mutex sync.Mutex
	// Your definitions here.
	mapTasks []MapTask
	reduceTasks []ReduceTask
	nReduce int
	MapDone bool
	ReduceDone bool
	workers []*TaskWorker
}

// task structure
type MapTask struct {
	// 1=idle 2=processing 3=success
	State int
	// task name = file name
	FileName string

	// worker reference
	Worker *TaskWorker
}


func (m *Master) appendWorker(w *TaskWorker) {
	isContain := false
	for _, worker := range m.workers {
		if worker != nil && worker.equals(w) {
			isContain = true
		}
	}

	if !isContain {
		m.workers = append(m.workers, w)
	}
}

type ReduceTask struct {
	// 1=idle 2=processing 3=success
	State int
	// task name = file name
	fileName string

	// worker reference
	Worker *TaskWorker

}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetMapTask(args *ExampleArgs, reply *TaskWorker) error {
	task := MapTask{
		State:    2,
		FileName: "pg-being_ernest.txt",
		Worker:   reply,
	}
	_ = append(m.mapTasks, task)

	reply.FileName = "pg-being_ernest.txt"
	//hardcode
	reply.NReduce = 10
	reply.TaskNo = 1
	return nil
}

func (m *Master) CallForTask(args *ExampleArgs, reply *TaskWorker) error {
	if !m.MapDone {
		// lock task list
		m.mutex.Lock()
		for index, mapTask := range m.mapTasks {
			if mapTask.State == 1 {
				reply.FileName = mapTask.FileName
				reply.NReduce = m.nReduce
				reply.TaskNo = index
				reply.IsMapTask = true
				reply.ReceiveTimestamp = time.Now().Unix()
				// set state to processing
				m.mapTasks[index].State = 2
				m.mapTasks[index].Worker = reply

				// append worker to workers list
				m.appendWorker(reply)
				break
			}

		}

		m.mutex.Unlock()
		return nil
	}

	if !m.ReduceDone {
		// lock task list
		m.mutex.Lock()
		for index, reduceTask := range m.reduceTasks {
			if reduceTask.State == 1 {
				reply.FileName = "reduce"
				reply.NReduce = m.nReduce
				reply.TaskNo = index
				reply.IsReduceTask = true
				reply.ReceiveTimestamp = time.Now().Unix()
				// set state to processing
				m.reduceTasks[index].State = 2
				m.reduceTasks[index].Worker = reply

				// append worker to workers list
				m.appendWorker(reply)
				break
			}

		}

		m.mutex.Unlock()
		return nil
	}

	return nil
}

func (m *Master) CallForFinished(task *TaskWorker, reply *ExampleReply) error   {
	if task.IsMapTask {
		// 判断该任务worker时候已被干掉
		if m.mapTasks[task.TaskNo].Worker == nil {
			log.Printf("Receive a overdue worker message, No: %d. Ignore result.", task.TaskNo)
			return nil
		}
		m.mapTasks[task.TaskNo].State = 3
		m.checkMapTaskFinished()

		for index, worker := range m.workers {
			if worker != nil && worker.equals(task) {
				now := time.Now().Unix()
				taskType := ""
				if worker.IsMapTask {
					taskType = "Map"
				} else {
					taskType = "Reduce"
				}


				log.Printf("%s task takes %d senconds for finishied.", taskType, now - worker.ReceiveTimestamp)
				m.workers[index].ReceiveTimestamp = 0
			}
		}
	}

	if task.IsReduceTask {
		if m.reduceTasks[task.TaskNo].Worker == nil {
			log.Printf("Receive a overdue worker message, No: %d. Ignore result.", task.TaskNo)
			return nil
		}
		m.reduceTasks[task.TaskNo].State = 3
		m.checkReduceFinished()


		for index, worker := range m.workers {
			if worker != nil && worker.equals(task) {
				now := time.Now().Unix()
				taskType := ""
				if worker.IsMapTask {
					taskType = "Map"
				} else {
					taskType = "Reduce"
				}


				log.Printf("%s task takes %d senconds for finishied.", taskType, now - worker.ReceiveTimestamp)
				m.workers[index].ReceiveTimestamp = 0
			}
		}
	}

	// check if done
	return nil
}

func (m *Master) periodicallyKickOutWorker() {
	for true {
		m.mutex.Lock()
		for index, worker := range m.workers {
			if worker != nil && worker.ReceiveTimestamp > 0 {
				now := time.Now().Unix()
				if now - worker.ReceiveTimestamp > 10 {
					taskType := ""
					if worker.IsMapTask {
						taskType = "Map"
					} else {
						taskType = "Reduce"
					}
					log.Printf("%s worker is overdue, release task: %d.", taskType, worker.TaskNo)
					// release task, remove worker
					if worker.IsMapTask {
						m.mapTasks[worker.TaskNo].State = 1
						m.mapTasks[worker.TaskNo].Worker = nil
					} else {
						m.reduceTasks[worker.TaskNo].State = 1
						m.reduceTasks[worker.TaskNo].Worker = nil
					}
					m.workers[index] = nil
				}
			}
		}

		m.mutex.Unlock()

		time.Sleep(1000)
	}

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
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// check if All Map Task is Done
//
func (m *Master) checkMapTaskFinished() {
	isFinished := true
	for _, mapTask := range m.mapTasks {
		if mapTask.State != 3 {
			isFinished = false
		}
	}

	if isFinished {
		log.Printf("Map tasks All Done, start reduce")
		m.MapDone = true
	}
}

// check if All Map Task is Done
func (m *Master) checkReduceFinished() {
	isFinished := true
	for _, reduceTask := range m.reduceTasks {
		if reduceTask.State != 3 {
			isFinished = false
		}
	}

	if isFinished {
		log.Printf("All reduce tasks were Done, Misson complete.")
		m.ReduceDone = true
	}
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	if m.ReduceDone {
		return true
	}
	return false
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce: nReduce,
		MapDone: false,
		ReduceDone: false,
	}

	for _, s := range files {
		m.mapTasks = append(m.mapTasks, MapTask{1, s, nil})
	}

	for i := 0; i < nReduce; i++ {
		reduceTask := ReduceTask{1, "reduce", nil}
		m.reduceTasks = append(m.reduceTasks, reduceTask)
	}
	// create map file dir
	tempDir := "/home/luke/projects/go/6.824lab/src/main/mr-temp"
	_, err := os.Stat(tempDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(tempDir, os.ModePerm)
	}
	if err != nil {
		fmt.Printf("mkdir failed![%v]\n", err)
	}

	// kick out overdue worker
	go m.periodicallyKickOutWorker()
	// start master server
	m.server()
	return &m
}
