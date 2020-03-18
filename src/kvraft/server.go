package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	ClientId  int64
	RequestId int
	Key       string
	Value     string
}

type Result struct {
	opType    string
	ClientId  int64
	RequestId int
	Key       string
	Value     string
	Err       Err
	GetValue  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database map[string]string
	ack      map[int64]int
	messages map[int]chan Result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{
		OpType:    "Get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
	})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	if _, ok := kv.messages[index]; !ok {
		kv.messages[index] = make(chan Result, 1)
	}
	chanMsg := kv.messages[index] // messageChan for apply
	kv.mu.Unlock()

	select {
	case msg := <-chanMsg:
		if args.ClientId != msg.ClientId || args.RequestId != msg.RequestId {
			reply.WrongLeader = true
			return
		}
		reply.Err = msg.Err
		reply.Value = msg.GetValue
	case <-time.After(time.Second * 1):
		reply.WrongLeader = true
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{
		OpType:    args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
		Value:     args.Value,
	})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	if _, ok := kv.messages[index]; !ok {
		kv.messages[index] = make(chan Result, 1)

	}
	chanMsg := kv.messages[index]
	kv.mu.Unlock()

	select {
	case msg := <-chanMsg:
		if args.ClientId != msg.ClientId || args.RequestId != msg.RequestId {
			reply.WrongLeader = true
			return
		}
		reply.Err = msg.Err
	case <-time.After(time.Second * 1):
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.ack = make(map[int64]int)
	kv.messages = make(map[int]chan Result)
	go kv.Update()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	return kv
}

func (kv *KVServer) Update() {
	for {
		msg := <-kv.applyCh
		request := msg.Command.(Op)
		result := Result{
			opType:    request.OpType,
			ClientId:  request.ClientId,
			RequestId: request.RequestId,
			Key:       request.Key,
			Value:     request.Value,
		}
		duplicated := kv.IsDuplicated(request.ClientId, request.RequestId)

		kv.mu.Lock()
		if request.OpType == "Get" {
			if v, ok := kv.database[request.Key]; ok {
				result.Err = OK
				result.GetValue = v
			} else {
				result.Err = ErrNoKey
				result.GetValue = ""
			}
		} else {
			if !duplicated {
				if request.OpType == "Put" {
					kv.database[request.Key] = request.Value
				} else {
					kv.database[request.Key] += request.Value
				}
			}
			result.Err = OK
		}
		if _, ok := kv.messages[msg.CommandIndex]; !ok {
			kv.messages[msg.CommandIndex] = make(chan Result, 1)
		}
		DPrintf("server%d process apply log %v", kv.me, result)
		kv.messages[msg.CommandIndex] <- result
		kv.mu.Unlock()
	}
}

func (kv *KVServer) IsDuplicated(clientId int64, requestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.ack[clientId]; ok && value >= requestId {
		return true
	}
	kv.ack[clientId] = requestId
	return false
}
