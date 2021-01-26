package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkerArgs struct {
	MapTaskNumber    int // finished maptask number
	ReduceTaskNumber int // finished reducetask number
}

type WorkerReply struct {
	Tasktype int // 0: map task, 1: reduce task, 2: waiting, 3: job finished
	NMap     int // number of map task
	NReduce  int // number of reduce task

	MapTaskNumber int    // map task only
	Filename      string // maptask only

	ReduceTaskNumber int // reducetask only
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
