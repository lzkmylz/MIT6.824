package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Master define necessary info
type Master struct {
	// Your definitions here.
	MapFiles     map[string]int
	MapState     map[string]string
	MapTasks     chan string
	ReduceTasks  chan int
	ReduceState  map[int]string
	NReduce      int
	MapFinish    bool
	ReduceFinish bool
	TotalFiles   int
	stateMutex   *sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

// MyRPC handle rpc call between master and worker
func (m *Master) MyRPC(args *MyArgs, reply *MyReply) error {
	switch args.Type {
	case "askTask":
		select {
		case maptask := <-m.MapTasks:
			reply.Type = "map"
			reply.MapFilename = maptask
			reply.NReduce = m.NReduce
			reply.MapIndex = m.MapFiles[maptask]
			go m.MapTaskHeartBeat(maptask)
			return nil
		case reducetask := <-m.ReduceTasks:
			reply.Type = "reduce"
			reply.ReduceIndex = reducetask
			reply.NReduce = m.NReduce
			reply.TotalFiles = m.TotalFiles
			go m.ReduceTaskHeartBeat(reducetask)
			return nil
		}
	case "reportTask":
		if args.MapFilename != "" {
			m.stateMutex.Lock()
			defer m.stateMutex.Unlock()
			m.MapState[args.MapFilename] = "finish"
			return nil
		} else if args.ReduceIndex >= 0 {
			m.stateMutex.Lock()
			defer m.stateMutex.Unlock()
			m.ReduceState[args.ReduceIndex] = "finish"
			return nil
		}
	}
	return nil
}

// MapTaskHeartBeat check if map task has finish or timeout
func (m *Master) MapTaskHeartBeat(filename string) {
	time.Sleep(time.Second * 3)
	m.stateMutex.Lock()
	defer m.stateMutex.Unlock()
	if m.MapState[filename] != "finish" {
		m.MapState[filename] = "unAsign"
		m.MapTasks <- filename
	}
}

// ReduceTaskHeartBeat check if reduce task timeout
func (m *Master) ReduceTaskHeartBeat(index int) {
	time.Sleep(time.Second * 3)
	m.stateMutex.Lock()
	defer m.stateMutex.Unlock()
	if m.ReduceState[index] != "finish" {
		m.ReduceState[index] = "unAsign"
		m.ReduceTasks <- index
	}
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
	go m.generateTasks()
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) generateTasks() {
	for k := range m.MapFiles {
		if m.MapState[k] == "unAsign" {
			m.MapTasks <- k
		}
	}
	ok := false
	for !ok {
		time.Sleep(time.Second)
		ok = m.checkMapFinish()
	}
	m.MapFinish = true

	for k := range m.ReduceState {
		m.ReduceTasks <- k
	}
	reduceOk := false
	for !reduceOk {
		time.Sleep(time.Second)
		reduceOk = m.checkReduceFinish()
	}
	m.ReduceFinish = reduceOk
}

func (m *Master) checkMapFinish() bool {
	m.stateMutex.RLock()
	defer m.stateMutex.RUnlock()
	for k := range m.MapFiles {
		if m.MapState[k] != "finish" {
			return false
		}
	}
	return true
}

func (m *Master) checkReduceFinish() bool {
	m.stateMutex.RLock()
	defer m.stateMutex.RUnlock()
	for _, v := range m.ReduceState {
		if v != "finish" {
			return false
		}
	}
	return true
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//

// Done tell mrmaster all works are done
func (m *Master) Done() bool {
	return m.ReduceFinish
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

// MakeMaster create Master instance
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	mapFiles := make(map[string]int)
	mapState := make(map[string]string)
	reduceState := make(map[int]string)
	mapTasks := make(chan string, 5)
	reduceTasks := make(chan int, 5)
	for k, v := range files {
		mapFiles[v] = k
		mapState[v] = "unAsign"
	}
	for i := 0; i < nReduce; i++ {
		reduceState[i] = "unAsign"
	}
	m.NReduce = nReduce
	m.MapFiles = mapFiles
	m.MapState = mapState
	m.ReduceState = reduceState
	m.MapFinish = false
	m.ReduceFinish = false
	m.stateMutex = new(sync.RWMutex)
	m.MapTasks = mapTasks
	m.ReduceTasks = reduceTasks
	m.TotalFiles = len(files)
	m.server()
	return &m
}
