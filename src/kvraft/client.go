package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	leader    int
	clientId  int64
	requestId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.requestId = 0
	ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: ck.requestId}
	ck.requestId++

	ret := ""
	for i := ck.leader; true; i = (i + 1) % len(ck.servers) {
		reply := GetReply{}
		server := ck.servers[i]
		if server.Call("KVServer.Get", &args, &reply) {
			if !reply.WrongLeader {
				ck.leader = i
				if reply.Err == OK {
					//DPrintf("[%d] Receive GET reply with [%d]", ck.clientId, args.RequestId)
					ret = reply.Value
					break
				} else {
					ret = ""
					break
				}
			}
		}
	}
	return ret
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, RequestId: ck.requestId}
	ck.requestId++

	for i := ck.leader; true; i = (i + 1) % len(ck.servers) {
		//!!! Attention: where to declare a variable
		reply := PutAppendReply{}
		server := ck.servers[i]
		if server.Call("KVServer.PutAppend", &args, &reply) {
			if !reply.WrongLeader {
				//DPrintf("[%d] Receive PUTAPPEND reply with [%d]", ck.clientId, args.RequestId)
				ck.leader = i
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
