package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	MAP = iota
	REDUCE
	WAITING
	RUNNING
	FINISHED
)

// Job is assigned a unique id, list of files to process and a status
// Status is assigned the value WAITING, RUNNING, FINISHED
type Job struct {
	Index  int
	Status int
	Files  []string
}

type Coordinator struct {
	status          int
	nMap            int
	remainingMap    int
	nReduce         int
	remainingReduce int
	mapJobs         []Job
	reduceJobs      []Job
	lock            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.status = MAP
	c.nMap = len(files)
	c.remainingMap = c.nMap
	c.nReduce = nReduce
	c.remainingReduce = c.nReduce
	c.mapJobs = make([]Job, c.nMap)
	c.reduceJobs = make([]Job, c.nReduce)

	for index, file := range files {
		log.Printf("loading - %s", file)
		c.mapJobs[index] = Job{index, WAITING, []string{file}}
	}
	for index := range c.reduceJobs {
		c.reduceJobs[index] = Job{index, WAITING, []string{}}
	}

	log.Println("making a new coordinator")

	c.server()
	return &c
}

func (c *Coordinator) AcquireJob(args *JobArgs, reply *JobReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	// The index value is used to acquire a new job or commit a completed job.
	// As part of committing the map jobs, get the names of files that have to be sent to reduce.
	if args.Job.Index >= 0 {
		// Commit only a job that is running. For cases where the job may be started in multiple workers.
		// If a map job completed, extract the list of file names to be reduced
		// Update the count of number of jobs.
		// If all jobs of a type are completed, update the coordinator status to the next step in the process.
		if args.Status == MAP {
			if c.mapJobs[args.Job.Index].Status == RUNNING {
				c.mapJobs[args.Job.Index].Status = FINISHED
				c.mapJobs[args.Job.Index].Files = append(c.mapJobs[args.Job.Index].Files, args.Job.Files...)
				c.remainingMap--
			}
			if c.remainingMap == 0 {
				c.status = REDUCE
			}
		} else {
			if c.reduceJobs[args.Job.Index].Status == RUNNING {
				c.reduceJobs[args.Job.Index].Status = FINISHED
				c.remainingReduce--
			}
			if c.remainingReduce == 0 {
				c.status = FINISHED
			}
		}
	}

	// Handout jobs to the workers if there are remaining jobs.
	// If the index is greater than 0, then commit the job as completed
	// If the index is less than 0, based on the status of the coordinator, return a job.
	if c.status == MAP {
		for index, job := range c.mapJobs {
			if job.Status == WAITING {
				log.Println("assigning a new job")
				reply.Status = MAP
				reply.NOther = c.nReduce
				reply.Job = job
				c.mapJobs[index].Status = RUNNING
				// TODO - include a timer that checks if the job is still running.
				return nil
			}
			reply.NOther = c.nReduce
			reply.Status = MAP
			reply.Job = Job{-1, WAITING, make([]string, 0)}
		}
	}

	if c.status == REDUCE {
		for index, job := range c.reduceJobs {
			if job.Status == WAITING {
				reply.Status = REDUCE
				reply.NOther = c.nMap
				reply.Job = job
				c.reduceJobs[index].Status = RUNNING
				// TODO - include a timer that checks for status of a job
				return nil
			}
			reply.NOther = c.nMap
			reply.Status = REDUCE
			reply.Job = Job{-1, WAITING, make([]string, 0)}
		}
	}
	return nil
}
