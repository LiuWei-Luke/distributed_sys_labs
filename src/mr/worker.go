package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

type WorkerState struct {
	IsProcessing bool
	task *TaskWorker
}

func (worker *WorkerState) heartBeat() {
	if worker.IsProcessing {
		// send heartBeat to master
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//worker := WorkerState{false, nil}

	// due unix signal
	//sig := make(chan os.Signal, 1)
	//done := make(chan bool, 1)
	//signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	//go func() {
	//	<-done
	//	fmt.Printf("Receice sigterm.")
	//	done <- true
	//}()
	//
	//<-done
	fmt.Printf("Starting a new worker.\n")
	// sleep 3 secs for a loop, then asking job from master
	for {
		CallTask(mapf, reducef)
		time.Sleep(3 * time.Second)
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

func CallTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := TaskWorker{}
	args := ExampleArgs{}
	call("Master.CallForTask", &args, &reply)
	if reply.FileName == "" {
		log.Printf("didn't get a task, waiting for 3 secs...")
		return
	}
	// sleep 2 seconds
	time.Sleep(1000)
	if reply.IsMapTask {
		mapTask(&reply, mapf)
	} else {
		reduceTask(&reply, reducef)
	}
}

func mapTask(task *TaskWorker, mapf func(string, string) []KeyValue,) {
	log.Printf("Receive a map task from master, file: %s.\n", task.FileName)
	// read file
	intermediate := []KeyValue{}
	rep := ExampleReply{}
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}

	// read content once all, write to tamp file
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}

	_ = file.Close()
	kva := mapf(task.FileName, string(content))
	intermediate = append(intermediate, kva...)

	// split map into n part map
	mapArr := [10][]KeyValue{}
	for _, kv := range intermediate {
		index := ihash(kv.Key) % task.NReduce
		mapArr[index] = append(mapArr[index], kv)
	}

	i := 1
	for _, kva := range mapArr {
		sort.Sort(ByKey(kva))
		fileName := fmt.Sprintf("mr-%d-%d", task.TaskNo + 1, i)
		tampFileName := "/home/luke/projects/go/6.824lab/src/main/mr-temp"
		tampFile, _ := ioutil.TempFile(tampFileName, "tmpfile")
		enc := json.NewEncoder(tampFile)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write json file %v", tampFileName)
			}
		}

		err := os.Rename(tampFile.Name(), "/home/luke/projects/go/6.824lab/src/main/mr-temp/" + fileName)
		tampFile.Close()
		if err != nil {
			log.Fatalf(err.Error())
		}
 		i++
	}

	// send finished call
	call("Master.CallForFinished", task, &rep)
}

func reduceTask(task *TaskWorker, reducef func(string, []string) string) {
	log.Printf("Receive a reduce task from master, No: %d\n", task.TaskNo)
	kva := []KeyValue{}
	for i := 0; i < 8; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i+1, task.TaskNo+1)
		fileName = "/home/luke/projects/go/6.824lab/src/main/mr-temp/" + fileName
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}

		// read content once all, append to kva
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// reduce
	sort.Sort(ByKey(kva))

	fileName := fmt.Sprintf("mr-out-%d", task.TaskNo + 1)
	tempDir := "/home/luke/projects/go/6.824lab/src/main/mr-tmp"
	tampFile, _ := ioutil.TempFile(tempDir, "tmpfile")

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tampFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	os.Rename(tampFile.Name(), "/home/luke/projects/go/6.824lab/src/main/mr-tmp/" + fileName)

	tampFile.Close()

	// send finished call
	rep := ExampleReply{}
	call("Master.CallForFinished", task, &rep)
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
