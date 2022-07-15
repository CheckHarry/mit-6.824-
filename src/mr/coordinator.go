package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type WorkerStatus struct {
	last_response int64
	state         int
	current_task  TaskInfo // we use a integer to identify a task
}

type TaskInfo struct {
	filename string
	stage    int
}

type Coordinator struct {
	// Your definitions here.
	mapdone        bool
	reducedone     bool
	done           bool
	files          []string
	mapoutputfiles []string
	nReduce        int
	stage          int
	count          int
	workerstatus   map[int]*WorkerStatus
	totaltask      int
	nowtotal       int
	mu             sync.Mutex
	queue          []TaskInfo
	logfile        *os.File
}

// Your code here -- RPC handlers for the worker to call.

const StageMap = 1
const StageReduce = 2

const StatusRunning = 1
const StatusKill = -1
const StatusDone = 0

func (c *Coordinator) RPChandler(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check if the queue have element
	//fmt.Println("RPChandler")
	if len(c.queue) == 0 {
		reply.Stage = 0
	} else {
		task := c.queue[0] // get front element
		// pop it out
		// fmt.Println("task is " + task.filename)
		c.queue = c.queue[1:]
		reply.Stage = c.stage
		reply.NReduce = c.nReduce
		reply.Filename = filepath.Base(task.filename)
		reply.Filepath = task.filename
		reply.Taskno = c.count
		c.workerstatus[args.Id] = &WorkerStatus{time.Now().Unix(), StatusRunning, task}
		c.count++
		fmt.Fprintf(c.logfile, "%d pid worker start %d task - %s\n", args.Id, reply.Stage, task.filename)
	}

	return nil
}

func (c *Coordinator) RPCHealthCheck(args *Args, reply *Reply) error { // receive a health check and update status
	// we use the pid from Args for identify the status of each
	id := args.Id
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workerstatus[id].last_response = time.Now().Unix()
	return nil
}

func (c *Coordinator) Report(args *ArgsReport, reply *ReplyReport) error {
	id := args.Id
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workerstatus[id].last_response = time.Now().Unix()
	c.workerstatus[id].state = StatusDone
	stage := c.workerstatus[id].current_task.stage
	c.nowtotal++
	//fmt.Printf("%d\n", len(args.Outputfiles))
	c.mapoutputfiles = append(c.mapoutputfiles, args.Outputfiles...)

	fmt.Fprintf(c.logfile, "%d pid word finish %d task - %s\n", args.Id, stage, c.workerstatus[id].current_task.filename)
	return nil
}

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
	fmt.Println(sockname)
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
	ret := c.done

	// Your code here.
	//time.Sleep(30000)
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

var Threshold = 10 * int64(time.Second)

func (c *Coordinator) healthcheck(Stage int) {
	for {
		c.mu.Lock()
		for key, value := range c.workerstatus {
			//fmt.Printf("%d , %d : %d \n", time.Now().Unix(), value.last_response, time.Now().Unix()-value.last_response)
			if time.Now().Unix()-value.last_response > 10 && value.state == StatusRunning {
				value.state = StatusKill
				c.queue = append(c.queue, value.current_task)
				fmt.Fprintf(c.logfile, "%d pid worker get killed - %s\n", key, value.current_task.filename)
			}
		}
		if Stage == StageMap {
			if c.mapdone == true {
				c.mu.Unlock()
				return
			}
		}
		if Stage == StageReduce {
			if c.reducedone == true {
				c.mu.Unlock()
				return
			}
		}
		c.mu.Unlock()
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) donecheck(Stage int) {
	for {
		c.mu.Lock()
		//fmt.Printf("%d , %d \n", c.nowtotal, c.totaltask)
		if c.nowtotal == c.totaltask {
			if Stage == StageMap {
				c.mapdone = true
				c.mu.Unlock()
				return
			} else if Stage == StageReduce {
				c.reducedone = true
				c.mu.Unlock()
				return
			}
		}
		c.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce
	c.stage = StageMap
	c.workerstatus = make(map[int]*WorkerStatus)
	c.queue = make([]TaskInfo, 0)
	c.mapoutputfiles = make([]string, 0)
	c.logfile, _ = os.Create("logging.txt")
	// Your code here.
	// TODO : implement task distributing logic
	// initialize the queue

	for _, file := range files {
		t := TaskInfo{file, StageMap}
		c.queue = append(c.queue, t)
		c.totaltask++
	} // no concurrency right here
	/*
		for _, f := range c.queue {
			fmt.Printf("abc : %s\n", f.filename)
		}*/

	c.server() // start the RPC
	var wg sync.WaitGroup
	// TODO : implement a health check and failover
	wg.Add(2)
	go func() {
		c.healthcheck(StageMap)
		fmt.Printf("Finish healthcheck\n")
		wg.Done()
	}()
	go func() {
		c.donecheck(StageMap)
		fmt.Printf("Finish donecheck\n")
		wg.Done()
	}()
	wg.Wait()
	// TODO : implement stage transition logic
	fmt.Printf("here!\n")
	//fmt.Printf("%d\n", len(c.mapoutputfiles))
	//for _, outputfilename := range c.mapoutputfiles {
	//	fmt.Printf("%s\n", outputfilename)
	//}
	c.mu.Lock()
	c.stage = StageReduce
	c.totaltask = 0 // reset the totaltask
	c.nowtotal = 0  // reset also
	rgx := regexp.MustCompile("middle_(.*)_(.*)_(.*).txt")
	filemapping := make(map[int]*os.File)
	for i := 0; i < nReduce; i++ {
		filemapping[i], _ = os.Create(fmt.Sprintf("middle_%d.txt", i))
	}
	for _, outputfilename := range c.mapoutputfiles {
		res := rgx.FindStringSubmatch(outputfilename)
		i, _ := strconv.Atoi(res[3])
		content, _ := os.ReadFile(outputfilename)
		filemapping[i].Write(content)
	}
	for i := 0; i < nReduce; i++ {
		t := TaskInfo{filemapping[i].Name(), StageReduce}
		c.queue = append(c.queue, t)
		c.totaltask++
		filemapping[i].Close()
	}
	c.mu.Unlock()
	wg.Add(2)
	go func() {
		c.healthcheck(StageReduce)
		fmt.Printf("Finish healthcheck\n")
		wg.Done()
	}()
	go func() {
		c.donecheck(StageReduce)
		fmt.Printf("Finish donecheck\n")
		wg.Done()
	}()
	wg.Wait()
	c.mu.Lock()
	c.done = true
	c.mu.Unlock()
	fmt.Printf("FINISHED!")
	return &c
}
