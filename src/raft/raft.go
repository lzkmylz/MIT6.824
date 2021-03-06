package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"

//
// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

//
// Raft A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyChan chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int
	votedFor        int
	log             []ApplyMsg
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	state           string
	leaderID        int
	timeRecord      time.Time
	electionTimeout int64
	getVote         int
	lastLogIndex    int
	lastLogTerm     int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == "leader"
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.lastApplied)
	for i := 1; i <= rf.lastApplied; i++ {
		DPrintf("server%d reapply log %d", rf.me, i)
		rf.applyChan <- rf.log[i]
	}
	DPrintf("server%d read from persister, currentTerm %d, votedFor %d, log length %d",
		rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
}

//
// RequestVoteArgs RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs for AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []ApplyMsg
	LeaderCommit int
}

// AppendEntriesReply for AppendEntries RPC
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		rf.mu.Lock()
		rf.timeRecord = time.Now()
		rf.mu.Unlock()
		return
	}

	if rf.currentTerm < args.Term {
		if rf.lastLogTerm > args.LastLogTerm {
			rf.resetToFollower(args.Term, true)
			return
		}
		if rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex {
			rf.resetToFollower(args.Term, true)
			return
		}
		rf.voteToOther(args.CandidateID, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}
	if rf.currentTerm == args.Term {
		if rf.lastLogTerm > args.LastLogTerm {
			return
		}
		if rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex {
			return
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			rf.voteToOther(args.CandidateID, args.Term)
		}
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	reply.Term = rf.currentTerm
	rf.mu.Lock()
	rf.timeRecord = time.Now()
	rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		return
	}

	DPrintf("server%d get heartbeat from server%d, leader term %d, self term %d, is empty heartbeat: %t",
		rf.me, args.LeaderID, args.Term, rf.currentTerm, len(args.Entries) == 0)
	if rf.currentTerm < args.Term {
		rf.resetToFollower(args.Term, true)
	}
	rf.mu.Lock()
	rf.leaderID = args.LeaderID
	rf.timeRecord = time.Now()
	rf.mu.Unlock()
	syncStatus := rf.syncWithLeader(args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	DPrintf("server%d sync with leader server%d, leaderCommit %d, leader PL %d, leader PT %d, sync Result %t, entities length %d, after sync log length %d, self commitIndex %d",
		rf.me, args.LeaderID, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, syncStatus, len(args.Entries), len(rf.log), rf.commitIndex)
	if syncStatus {
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}
	reply.Term = rf.currentTerm
	reply.Success = false
}

func (rf *Raft) sendRequestVote(server int) {
	DPrintf("server%d request server%d to vote, term %d", rf.me, server, rf.currentTerm)
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogIndex = rf.lastLogIndex
	args.LastLogTerm = rf.lastLogTerm
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.resetToFollower(reply.Term, true)
			return
		}
		if !reply.VoteGranted {
			rf.mu.Lock()
			rf.timeRecord = time.Now()
			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted {
			DPrintf("server%d get vote from server%d, term %d",
				rf.me, server, rf.currentTerm)
			rf.getVoteFromOther()
		}
	}
}

func (rf *Raft) sendHeartBeat(server int) {
	if rf.state != "leader" {
		return
	}
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	args.Entries = []ApplyMsg{}
	args.LeaderCommit = rf.commitIndex
	args.LeaderID = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].CommandTerm
	args.Term = rf.currentTerm

	// if not sync with this server, send heartbeat with empty Entries
	if rf.nextIndex[server]-1 == rf.lastLogIndex {
		DPrintf("server%d send heartbeat to server%d, PT %d, PI %d",
			rf.me, server, args.PrevLogTerm, args.PrevLogIndex)
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		if ok {
			// hearbeat failed because of term, revert to follower
			if !reply.Success && rf.currentTerm < reply.Term {
				rf.resetToFollower(reply.Term, true)
				return
			}
			// if heartbeat failed because of inconsistencies
			if !reply.Success {
				rf.logInconsistencies(server, args.PrevLogTerm)
			}
			// heartbeat success
			if reply.Success {
				// check matchIndex
				rf.mu.Lock()
				rf.timeRecord = time.Now()
				rf.mu.Unlock()
				rf.syncWithServer(server, args.PrevLogIndex)
			}
		}
		return
	}

	// if already sync
	args.Entries = rf.log[rf.nextIndex[server]:len(rf.log)]
	recordLastLogIndex := rf.lastLogIndex
	DPrintf("server%d send heartbeat to server%d, PT %d, PI %d, sync nextIndex %d, leader commitIndex %d",
		rf.me, server, args.PrevLogTerm, args.PrevLogIndex, rf.nextIndex[server], rf.commitIndex)
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		// hearbeat failed because of term, revert to follower
		if !reply.Success && rf.currentTerm < reply.Term {
			rf.resetToFollower(reply.Term, true)
			return
		}
		// if heartbeat failed because of inconsistencies
		if !reply.Success {
			rf.logInconsistencies(server, args.PrevLogTerm)
		}
		// heartbeat success
		if reply.Success {
			// check matchIndex
			rf.mu.Lock()
			rf.timeRecord = time.Now()
			rf.mu.Unlock()
			rf.syncWithServer(server, recordLastLogIndex)
		}
	}
}

// Start get command from client
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.state == "leader"

	// Your code here (2B).
	index = rf.lastLogIndex + 1
	term = rf.currentTerm
	if !isLeader {
		return index, term, isLeader
	}
	log := ApplyMsg{
		Command:      command,
		CommandIndex: index,
		CommandTerm:  term,
		CommandValid: true,
	}

	rf.log = append(rf.log, log)
	rf.lastLogIndex = index
	rf.lastLogTerm = term
	rf.matchIndex[rf.me] = rf.lastLogIndex
	DPrintf("leader server%d get command, lastLogIndex %d, lastLogTerm %d",
		rf.me, rf.lastLogIndex, rf.lastLogTerm)
	return index, term, isLeader
}

//
// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(50 + int64(me)*10)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.mu = *new(sync.Mutex)
	rf.currentTerm = 0
	raftlog := []ApplyMsg{}
	raftlog = append(raftlog, ApplyMsg{
		CommandIndex: 0,
		CommandTerm:  0,
		CommandValid: true,
	})
	rf.log = raftlog
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.timeRecord = time.Now()
	rf.leaderID = -1
	rf.getVote = 0
	rf.resetToFollower(-1, true)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastLogIndex = rf.log[len(rf.log)-1].CommandIndex
	rf.lastLogTerm = rf.log[len(rf.log)-1].CommandTerm

	go rf.run()
	return rf
}

func (rf *Raft) run() {
	for {
		time.Sleep(time.Millisecond * 15)
		if rf.killed() {
			return
		}
		if int64(time.Since(rf.timeRecord)/time.Millisecond) > rf.electionTimeout {
			if rf.state == "follower" || rf.state == "candidate" {
				go rf.electLeader()
			} else {
				go rf.heartBeat()
			}
		}
	}
}

func (rf *Raft) electLeader() {
	rf.resetToCandidate()
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i)
		}
	}
}

func (rf *Raft) heartBeat() {
	DPrintf("server%d heartbeat, term %d, leader lastLogIndex %d, leaderCommit to %d",
		rf.me, rf.currentTerm, rf.lastLogIndex, rf.commitIndex)
	rf.mu.Lock()
	rf.timeRecord = time.Now()
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendHeartBeat(i)
		}
	}
}

func (rf *Raft) resetToFollower(resetTerm int, fast bool) {
	rf.mu.Lock()
	if resetTerm != -1 && resetTerm > rf.currentTerm {
		rf.currentTerm = resetTerm
		rf.persist()
	}
	if fast {
		rf.electionTimeout = rand.Int63n(200) + 100
	} else {
		rf.electionTimeout = rand.Int63n(300) + 300
	}
	rf.votedFor = -1
	rf.state = "follower"
	rf.timeRecord = time.Now()
	rf.getVote = 0
	rf.leaderID = -1
	rf.mu.Unlock()
}

func (rf *Raft) resetToCandidate() {
	rf.mu.Lock()
	rf.currentTerm = rf.currentTerm + 1
	rf.state = "candidate"
	rf.votedFor = rf.me
	rf.timeRecord = time.Now()
	rf.getVote = 1
	rf.leaderID = -1
	DPrintf("server%d become candidate, term %d", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	rf.persist()
}

func (rf *Raft) resetToLeader() {
	DPrintf("server%d become leader, term %d", rf.me, rf.currentTerm)
	rf.mu.Lock()
	rf.state = "leader"
	rf.leaderID = rf.me
	rf.timeRecord = time.Now()
	rf.electionTimeout = 100
	nextIndex := []int{}
	matchIndex := []int{}
	for range rf.peers {
		nextIndex = append(nextIndex, rf.lastLogIndex+1)
		matchIndex = append(matchIndex, 0)
	}
	matchIndex[rf.me] = rf.lastLogIndex
	rf.nextIndex = nextIndex
	rf.matchIndex = matchIndex
	rf.mu.Unlock()
}

func (rf *Raft) syncWithServer(server, matchAtIndex int) {
	rf.mu.Lock()
	if rf.matchIndex[server] < matchAtIndex {
		rf.matchIndex[server] = matchAtIndex

		// count matchIndex with hash map
		matchCopy := []int{}
		for _, v := range rf.matchIndex {
			matchCopy = append(matchCopy, v)
		}
		sort.Ints(matchCopy[:])
		majority := matchCopy[len(matchCopy)/2]
		DPrintf("leader server%d update matchIndex %v", rf.me, rf.matchIndex)

		if majority > rf.commitIndex {
			rf.commitIndex = majority
			for i := rf.lastApplied + 1; i <= majority; i++ {
				DPrintf("leader server%d apply log %d, commitIndex %d", rf.me, i, rf.commitIndex)
				rf.applyChan <- rf.log[i]
				rf.lastApplied = i
			}
		}
	}
	rf.nextIndex[server] = matchAtIndex + 1
	rf.mu.Unlock()
	rf.persist()
}

func (rf *Raft) getVoteFromOther() {
	rf.mu.Lock()
	rf.getVote = rf.getVote + 1
	rf.mu.Unlock()
	if rf.getVote >= len(rf.peers)/2+1 && rf.state == "candidate" {
		rf.resetToLeader()
		rf.heartBeat()
	}
}

func (rf *Raft) voteToOther(server, newTerm int) {
	rf.resetToFollower(newTerm, false)
	rf.mu.Lock()
	rf.votedFor = server
	rf.mu.Unlock()
	DPrintf("server%d vote to server%d, term %d",
		rf.me, server, rf.currentTerm)
	rf.persist()
}

func (rf *Raft) logInconsistencies(server, lastHBTerm int) {
	recordIndex := 0
	recordTerm := 0
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].CommandTerm < lastHBTerm && rf.log[i].CommandTerm > recordTerm && i > recordIndex {
			recordIndex = i
			recordTerm = rf.log[i].CommandTerm
		}
	}
	rf.mu.Lock()
	if recordIndex == 0 {
		rf.nextIndex[server] = 1
	} else {
		rf.nextIndex[server] = recordIndex
	}
	rf.mu.Unlock()
	DPrintf("leader%d try consistence with server%d, lastHBTerm %d, after consistence nextIndex %d, term %d",
		rf.me, server, lastHBTerm, recordIndex, recordTerm)
}

func (rf *Raft) syncWithLeader(leaderCommit, checkIndex, checkTerm int, entries []ApplyMsg) bool {
	checkResult := false
	DPrintf(`server%d sync with leader function, leaderCommit %d, checkIndex %d,
	checkTerm %d, entries length %d`,
		rf.me, leaderCommit, checkIndex, checkTerm, len(entries))
	if checkTerm > rf.lastLogTerm || checkIndex > rf.lastLogIndex {
		DPrintf("check failed because of checkTerm %d > rf.lastLogTerm %d || checkIndex %d > rf.lastLogIndex %d, self commitIndex %d, self lastLogIndex %d",
			checkTerm, rf.lastLogTerm, checkIndex, rf.lastLogIndex, rf.commitIndex, rf.lastLogIndex)
		return checkResult
	}
	if rf.log[checkIndex].CommandTerm != checkTerm {
		DPrintf("check failed because rf.log[checkIndex].CommandTerm != checkTerm")
		rf.mu.Lock()
		rf.log = rf.log[:rf.commitIndex+1]
		rf.lastLogIndex = rf.log[len(rf.log)-1].CommandIndex
		rf.lastLogTerm = rf.log[len(rf.log)-1].CommandTerm
		rf.mu.Unlock()
		rf.persist()
		return checkResult
	}
	checkResult = true
	if checkResult {
		rf.mu.Lock()
		// find a match point
		if len(entries) > 0 {
			copyStartPoint := entries[0].CommandIndex

			if copyStartPoint > rf.lastLogIndex {
				for _, v := range entries {
					rf.log = append(rf.log, v)
				}
			} else {
				rf.log = rf.log[:copyStartPoint]
				for _, v := range entries {
					rf.log = append(rf.log, v)
				}
			}
			rf.persist()
		} else {
			if rf.lastLogIndex > checkIndex {
				rf.log = rf.log[:checkIndex+1]
				rf.persist()
			}
		}

		rf.lastLogIndex = rf.log[len(rf.log)-1].CommandIndex
		rf.lastLogTerm = rf.log[len(rf.log)-1].CommandTerm
		rf.commitIndex = leaderCommit

		if len(entries) > 0 {
			for i := rf.lastApplied + 1; i <= leaderCommit; i++ {
				rf.applyChan <- rf.log[i]
				rf.lastApplied = i
				DPrintf("server%d apply log %d", rf.me, rf.lastApplied)
			}
			rf.persist()
		} else if rf.lastLogIndex == checkIndex && rf.lastLogTerm == checkTerm {
			for i := rf.lastApplied + 1; i <= leaderCommit; i++ {
				rf.applyChan <- rf.log[i]
				rf.lastApplied = i
				DPrintf("server%d apply log %d", rf.me, rf.lastApplied)
			}
			rf.persist()
		} else {
			if rf.lastApplied+1 <= leaderCommit && rf.lastApplied+1 <= rf.lastLogIndex {
				rf.applyChan <- rf.log[rf.lastApplied+1]
				rf.lastApplied = rf.lastApplied + 1
				DPrintf("server%d apply log %d", rf.me, rf.lastApplied)
				rf.persist()
			}
		}

		rf.mu.Unlock()
	}
	return checkResult
}
