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

type Master struct {
	// Your definitions here.
	nReduce        int // number of reduce task
	nMap           int // number of map task
	files          []string
	mapfinished    int        // number of finished map task
	maptasklog     []int      // log for map task, 0: not allocated, 1: waiting, 2:finished
	reducefinished int        // number of finished map task
	reducetasklog  []int      // log for reduce task
	mu             sync.Mutex // lock
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) ReceiveFinishedMap(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	m.mapfinished++
	m.maptasklog[args.MapTaskNumber] = 2 // log the map task as finished
	m.mu.Unlock()
	return nil
}

func (m *Master) ReceiveFinishedReduce(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	m.reducefinished++
	m.reducetasklog[args.ReduceTaskNumber] = 2 // log the reduce task as finished
	m.mu.Unlock()
	return nil
}

func (m *Master) AllocateTask(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	if m.mapfinished < m.nMap {
		// allocate new map task
		allocate := -1
		for i := 0; i < m.nMap; i++ {
			if m.maptasklog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			// waiting for unfinished map jobs
			reply.Tasktype = 2
			m.mu.Unlock()
		} else {
			// allocate map jobs
			reply.NReduce = m.nReduce
			reply.Tasktype = 0
			reply.MapTaskNumber = allocate
			reply.Filename = m.files[allocate]
			m.maptasklog[allocate] = 1 // waiting
			m.mu.Unlock()              // avoid deadlock
			go func() {
				time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
				m.mu.Lock()
				if m.maptasklog[allocate] == 1 {
					// still waiting, assume the map worker is died
					m.maptasklog[allocate] = 0
				}
				m.mu.Unlock()
			}()
		}
	} else if m.mapfinished == m.nMap && m.reducefinished < m.nReduce {
		// allocate new reduce task
		allocate := -1
		for i := 0; i < m.nReduce; i++ {
			if m.reducetasklog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			// waiting for unfinished reduce jobs
			reply.Tasktype = 2
			m.mu.Unlock()
		} else {
			// allocate reduce jobs
			reply.NMap = m.nMap
			reply.Tasktype = 1
			reply.ReduceTaskNumber = allocate
			m.reducetasklog[allocate] = 1 // waiting
			m.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
				m.mu.Lock()
				if m.reducetasklog[allocate] == 1 {
					// still waiting, assume the reduce worker is died
					m.reducetasklog[allocate] = 0
				}
				m.mu.Unlock()
			}()
		}
	} else {
		reply.Tasktype = 3
		m.mu.Unlock()
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
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// ret := m.mapfinished == m.nMap
	ret := m.reducefinished == m.nReduce
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.files = files
	m.nMap = len(files)
	m.nReduce = nReduce
	m.maptasklog = make([]int, m.nMap)
	m.reducetasklog = make([]int, m.nReduce)
	m.server()
	return &m
}
