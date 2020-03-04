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

// MyArgs use for rpc call args
type MyArgs struct {
	Type        string
	MapFilename string
	ReduceIndex int
}

// MyReply use for rpc call reply
type MyReply struct {
	Type        string
	MapFilename string
	ReduceIndex int
	NReduce     int
	MapIndex    int
	TotalFiles  int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
