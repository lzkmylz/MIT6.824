package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//

// KeyValue struce for map reduce model
type KeyValue struct {
	Key   string
	Value string
}

// ByKey define for sort KeyValue
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

// Worker run a worker instance
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for true {
		reply := CallForTask()
		if reply.Type == "" {
			break
		}
		switch reply.Type {
		case "map":
			doMap(&reply, mapf)
		case "reduce":
			doReduce(&reply, reducef)
		}
	}
}

// CallForTask rpc to master and ask for a task
func CallForTask() MyReply {
	args := MyArgs{}
	reply := MyReply{}
	args.Type = "askTask"
	res := call("Master.MyRPC", &args, &reply)
	if !res {
		fmt.Printf("get task error")
		return MyReply{Type: ""}
	}
	return reply
}

func doMap(reply *MyReply, mapf func(string, string) []KeyValue) {
	filename := reply.MapFilename
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))
	kvas := Partition(kva, reply.NReduce)
	// write to temp local file
	for i := 0; i < reply.NReduce; i++ {
		saveFilename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.MapIndex)
		interfile, _ := os.Create(saveFilename)
		enc := json.NewEncoder(interfile)
		for _, kv := range kvas[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("error: ", err)
			}
		}
	}
	reportArgs := MyArgs{}
	reportReply := MyReply{}
	reportArgs.Type = "reportTask"
	reportArgs.MapFilename = reply.MapFilename
	call("Master.MyRPC", &reportArgs, &reportReply)
}

func doReduce(reply *MyReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < reply.TotalFiles; i++ {
		filename := "mr-" + strconv.Itoa(reply.ReduceIndex) + "-" + strconv.Itoa(i)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reply.ReduceIndex)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()

	reportArgs := MyArgs{}
	reportReply := MyReply{}
	reportArgs.Type = "reportTask"
	reportArgs.ReduceIndex = reply.ReduceIndex
	call("Master.MyRPC", &reportArgs, &reportReply)
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

// Partition divide KeyValue to nReduce buckets
func Partition(kva []KeyValue, nReduce int) [][]KeyValue {
	kvas := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		v := ihash(kv.Key) % nReduce
		kvas[v] = append(kvas[v], kv)
	}
	return kvas
}
