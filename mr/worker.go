package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// When a worker comes up construct the args with a default structure.
	args := JobArgs{Job{Index: -1}, MAP}
	for {
		// CallExample()
		reply := JobReply{}
		// Acquire a new job from the coordinator
		call("Coordinator.AcquireJob", &args, &reply)
		// If the status of the reply is FINISHED, all jobs have completed execution. Break.
		if reply.Status == FINISHED {
			break
		}
		// If the reply job has an index greater than 0 a new job is assigned.
		// Finish and commit the job.
		if reply.Job.Index >= 0 {
			// Create a structure to commit the completed job
			commitJob := reply.Job
			// The reply job is either a map or reduce.
			if reply.Status == MAP {
				// Assign the files to reduce to the commitJob files
				commitJob.Files = doMap()
			} else {
				doReduce()
				commitJob.Files = make([]string, 0)
			}
			// The args is assigned the commit information.
			args = JobArgs{commitJob, reply.Status}
		} else {
			// If no jobs are provided, wait.
			time.Sleep(time.Second)
			args = JobArgs{Job{Index: -1}, MAP}
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

// Once a new job is acquired, the worker does a map or a reduce.
func doMap() []string {
	resultFiles := make([]string, 0)
	return resultFiles
}

func doReduce() {}
