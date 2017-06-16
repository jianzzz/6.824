package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import (
//	"fmt"
	"sync"
	"labrpc"
	"bytes"
	"encoding/gob"
	"time"
	"math/rand"
) 
 
const(
	STATE_FOLLOWER = iota
	STATE_CANDIDATE
	STATE_LEADER

	HEARTBEATINTERVAL = 50 * time.Millisecond // 50ms
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	state int
	voteCount int

	chanSuccVote chan bool
	chanHeartBeat chan bool
	chanLeader chan bool 
	chanApplyMsg chan ApplyMsg
	chanCommit chan bool

	// Persistent state on all servers
	currentTerm	int //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor 	int //candidateId that received vote in current term (or null if none)
	log 		[]LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	// Volatile state on all servers
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// Volatile state on leaders
	nextIndex 	[]int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex 	[]int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

type LogEntry struct {
	Index 	int
	Term 	int
	Command interface{}
}

func (rf *Raft) getLastIndex() int{
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastTerm() int{
	return rf.log[len(rf.log)-1].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	var t int
	var b bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	t = rf.currentTerm
	b = rf.state == STATE_LEADER
	return t,b
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C). 
	// whoever calls persist() has already acquired lock
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	//do not persist commitIndex and lastApplied
	//e.Encode(rf.commitIndex)
	//e.Encode(rf.lastApplied)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C). 
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	//fmt.Println(rf.log)
	//d.Decode(&rf.commitIndex)
	//d.Decode(&rf.lastApplied)
}

//
// restore previously snapshot.
//
func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {   
		return
	}
	var lastIncludedIndex int
	var lastIncludedTerm int
	var ssData []byte

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)
	d.Decode(&ssData)

	//truncate log
	rf.truncateLog(lastIncludedIndex,lastIncludedTerm) 

	//send snapshot to tester or service
	var m ApplyMsg
	m.UseSnapshot = true
	m.Snapshot = ssData
	go func() {
		rf.chanApplyMsg <- m
	}()

	// update commitIndex and lastApplied   
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int//candidate’s term
	CandidateId 	int//candidate requesting vote
	LastLogIndex 	int//index of candidate’s last log entry 
	LastLogTerm 	int//term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int//currentTerm, for candidate to update itself
	VoteGranted bool//true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()	
	defer rf.mu.Unlock()

	defer rf.persist()

	reply.VoteGranted=false
	if rf.currentTerm > args.Term{
		reply.Term=rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	//if has had updated log
	/*
	Raft determines which of two logs is more up-to-date
	by comparing the index and term of the last entries in the
	logs. If the logs have last entries with different terms, then
	the log with the later term is more up-to-date. If the logs
	end with the same term, then whichever log is longer is
	more up-to-date
	*/
	candidateLogMoreUpToDate := true
	if rf.getLastTerm() > args.LastLogTerm || 
	   (rf.getLastTerm() == args.LastLogTerm && rf.getLastIndex() > args.LastLogIndex){
		candidateLogMoreUpToDate = false
	}

	//rf.votedFor = -1 means haven not vote in this same term, or a large term is comming
	//when will happen like : rf.votedFor == args.CandidateId and vote again?
	//if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptoDate {
	if rf.votedFor == -1 && candidateLogMoreUpToDate == true{
		rf.chanSuccVote <- true 
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId 
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()	
	defer rf.mu.Unlock()	
	if ok{
		if rf.state!=STATE_CANDIDATE{//broadcastRequestVote,may become follower already
			return ok
		}
		if args.Term!=rf.currentTerm{//broadcastRequestVote,may update term already
			return ok
		}
		if reply.Term>rf.currentTerm{
			rf.currentTerm=reply.Term
			rf.state=STATE_FOLLOWER
			rf.persist()
			return ok
		}
		if reply.VoteGranted{
			rf.voteCount++
			if rf.state==STATE_CANDIDATE && rf.voteCount*2 > len(rf.peers) { 
				rf.state = STATE_LEADER
				rf.chanLeader <- true
			}
		}
	}
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	var req RequestVoteArgs
	rf.mu.Lock()	
	defer rf.mu.Unlock() 

	//no need to broadcast
	if rf.state!=STATE_CANDIDATE {
		return
	}

	req.Term = rf.currentTerm
	req.CandidateId = rf.me 
	req.LastLogIndex = rf.getLastIndex()
	req.LastLogTerm = rf.getLastTerm()
 
	for i,_:=range rf.peers{
		if i!=rf.me && rf.state==STATE_CANDIDATE{
			go func(i int){
				var reply RequestVoteReply
				rf.sendRequestVote(i,&req,&reply)
			}(i)
		} 
	}
} 


type AppendEntriesArgs struct { 
	Term int //leader’s term
	LeaderId int //so follower can redirect clients
	PrevLogTerm int //index of log entry immediately preceding new ones
	PrevLogIndex int //term of prevLogIndex entry
	Entries []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int //leader’s commitIndex
}

type AppendEntriesReply struct { 
	Term int //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm 
	NextIndex int//tell leader to send entyies at NextIndex next time
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) { 
	rf.mu.Lock()	
	defer rf.mu.Unlock()

	defer rf.persist()

	reply.Success=false
	if rf.currentTerm > args.Term{
		reply.Term=rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
	}
	reply.Term = rf.currentTerm

	rf.chanHeartBeat <- true
	
	// if server's log contains an entry at prevLogIndex whose term matches prevLogTerm

	// even if the coming entries are empty,we should check prevLog's consistent, 
	// otherwise we may commit the old incorrect entry(when leader's commitIndex > current server's commitIndex) 
	if args.PrevLogIndex > rf.getLastIndex() {
		reply.NextIndex = rf.getLastIndex()+1
		return
	}

	smallestIndex := rf.log[0].Index
	if args.PrevLogIndex > smallestIndex{
		term := rf.log[args.PrevLogIndex-smallestIndex].Term
		if term!=args.PrevLogTerm{
			//tell leader to retry at nextIndex, an entry at prevLogIndex whose term doesnot match preLogTerm, so we can ingore all these entries
			//has the same term
			for i:=args.PrevLogIndex-1;i>=smallestIndex;i--{
				if rf.log[i-smallestIndex].Term != term{
					reply.NextIndex = i+1
					break
				}
			}
			return
		}
	}
		
	if args.PrevLogIndex<smallestIndex{
		//fmt.Println("in AppendEntries,args.PrevLogIndex, smallestIndex = ",args.PrevLogIndex, smallestIndex)
	}else{
		if len(args.Entries) > 0 {
			//see if an entry conflicts with the new one? no need, simply cut off and append
			rf.log = rf.log[:args.PrevLogIndex+1-smallestIndex]
			rf.log = append(rf.log,args.Entries...)
			//fmt.Println("in raft's AppendEntries,me,lastIndex,args.Entries =",rf.me,rf.getLastIndex(),args.Entries)
			reply.NextIndex = rf.getLastIndex()+1
		}
	}
	//fmt.Println("in raft's AppendEntries,me,lastIndex=",rf.me,rf.getLastIndex())

	//update server's commitIndex
	//attention: we should update commitIndex when receive heartbeat,
	//but not log entry!!! means if no new entry coming, may still update commitIndex
	//but!! at least leader's commitIndex is larger than current server's
	oldCommitIndex := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex{
		//fmt.Println("-- in raft's AppendEntries 1,me,commitIndex,lastIndex=",rf.me,rf.commitIndex,rf.getLastIndex())
		if args.LeaderCommit > rf.getLastIndex(){
			rf.commitIndex = rf.getLastIndex()
		}else{
			rf.commitIndex = args.LeaderCommit
		}
		//should tell service or tester we have commited new entry
		if oldCommitIndex < rf.commitIndex{
			rf.chanCommit <- true
		}
		//fmt.Println("-- in raft's AppendEntries 2,me,commitIndex,lastIndex=",rf.me,rf.commitIndex,rf.getLastIndex())
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()	
	defer rf.mu.Unlock()	
	if ok{
		if rf.state!=STATE_LEADER{//broadcastAppendEntries,may become follower already
			return ok
		}
		if args.Term!=rf.currentTerm{//broadcastAppendEntries,may update term already
			return ok
		}
		if reply.Term>rf.currentTerm{
			rf.currentTerm=reply.Term
			rf.state=STATE_FOLLOWER
			rf.persist()
			return ok
		}
		if reply.Success{
			// update nextIndex and matchIndex
			if len(args.Entries)>0{
				// Oh oh! Pay attention here!
				// You may think that it should update rf.matchIndex[server] even when args.Entries are empty. Why?
				// Think about this: At the first beginning, leader A receives 5 commands, appends to its log, and sends log to others server.
				// All servers accept the new log, and after that, leader A crashes. B becomes the new leader, its matchIndex[i] is 0.
				// However, leader B has no need to send log(i.e. args.Entries are empty), cause the logs for each server are the same. 
				// If we don't update matchIndex[i], these logs may never be committed.
				// 
				// That seems to be true. But you may ingore the fact that we won't update commitIndex until we accept new logs in current term.
				// It is meaningless if only update the matchIndex[i].
				// On the other hand, if we have accepted new log in current term already, at some point len(args.Entries) will be large than 0.
				// So it is OK to update matchIndex[i] when len(args.Entries) is large than 0.
				//
				// but we can still update matchIndex when len(args.Entries) is 0
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1 
			} 					
			rf.matchIndex[server] = rf.nextIndex[server]-1	
			//fmt.Println("in raft's sendAppendEntries,me,term,server,matchIndex=",rf.me,rf.currentTerm,server,rf.matchIndex[server]) 
		}else{
			//update nextIndex, means decrement nextIndex and try it next time
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()	
	defer rf.mu.Unlock() 

	//no need to broadcast
	if rf.state!=STATE_LEADER {
		return
	}

	// update leader's commitIndex
	smallestIndex := rf.log[0].Index
	N := rf.commitIndex
	for i:= rf.getLastIndex(); i>rf.commitIndex; i--{
		num := 1
		for j := range rf.peers{
			if j!=rf.me && rf.matchIndex[j]>=i && i>=smallestIndex && rf.log[i-smallestIndex].Term==rf.currentTerm{
				num++
			}
		}
		if num*2>len(rf.peers){
			N=i
			break
		}
	}
	if N!=rf.commitIndex{
		//fmt.Println("in raft's broadcastAppendEntries,me,term,commitIndex,newCommitIndex,lastIndex=",rf.me,rf.currentTerm,rf.commitIndex,N,rf.getLastIndex())
 
		rf.commitIndex = N
		rf.chanCommit <- true

		rf.persist()
	}
	for i,_:=range rf.peers{
		if i!=rf.me && rf.state==STATE_LEADER { 
			if rf.nextIndex[i] > smallestIndex{
				var req AppendEntriesArgs
				req.Term = rf.currentTerm
				req.LeaderId = rf.me
				req.LeaderCommit = rf.commitIndex
				req.PrevLogIndex = rf.nextIndex[i]-1
				req.PrevLogTerm = rf.log[req.PrevLogIndex-smallestIndex].Term
				count := len(rf.log[rf.nextIndex[i]-smallestIndex:])
				req.Entries = make([]LogEntry,count)
				//send all the rest of the log
				copy(req.Entries,rf.log[rf.nextIndex[i]-smallestIndex:])
				go func(i int){
					var reply AppendEntriesReply
					rf.sendAppendEntries(i,&req,&reply)
				}(i)
			}else{
			/*
				Although servers normally take snapshots independently, the leader must occasionally send snapshots to
				followers that lag behind. This happens when the leader has already discarded the next log entry that it needs to
				send to a follower. Fortunately, this situation is unlikely in normal operation: a follower that has kept up with the
				leader would already have this entry. However, an exceptionally slow follower or a new server joining the cluster
				(Section 6) would not. The way to bring such a follower up-to-date is for the leader to send it a snapshot over the network.
			*/
				//fmt.Println("in broadcastAppendEntries, need to send Snapshot, leader, server, nextIndex, log[0].Index = ",rf.me,i,rf.nextIndex[i],rf.log[0].Index)
				var req InstallSnapshotArgs
				req.Term = rf.currentTerm
				req.LeaderId = rf.me
				req.LastIncludedIndex = rf.log[0].Index
				req.LastIncludedTerm = rf.log[0].Term
				req.Data = rf.persister.ReadSnapshot()
				req.Offset = 0
				req.Done = true
				go func(i int){
					var reply InstallSnapshotReply
					rf.sendSnapshot(i,&req,&reply)
				}(i)
			}
		} 
	}
}



type InstallSnapshotArgs struct{
	Term int // leader’s term
	LeaderId int // so follower can redirect clients
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm int // term of lastIncludedIndex
	Offset int // byte offset where chunk is positioned in the snapshot file
	Data[] byte // raw bytes of the snapshot chunk, starting at offset
	Done bool // true if this is the last chunk
}

type InstallSnapshotReply struct{
	Term int // currentTerm, for leader to update itself
	NextIndex int//tell leader to send entyies at NextIndex next time
}

func (rf *Raft) truncateLog(lastIncludedIndex int,lastIncludedTerm int){
	var rest []LogEntry
	// the first log entry still is the old useless entry, just for the sake of computation
	var first LogEntry = LogEntry{Index:lastIncludedIndex,Term:lastIncludedTerm}
	rest = append(rest,first)
	end := len(rf.log)
	for i:=0;i<end;i++{
		if rf.log[i].Index==lastIncludedIndex && rf.log[i].Term==lastIncludedTerm{
			rest = append(rest,rf.log[i+1:]...)
			break
		}
	}
	rf.log = rest
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) { 
	//fmt.Println("in InstallSnapshot 1, me, lastIncludedIndex, log[0].Index, lastIndex = ",rf.me,args.LastIncludedIndex, rf.log[0].Index,rf.getLastIndex())
	rf.mu.Lock()	
	defer rf.mu.Unlock()
	//fmt.Println("in InstallSnapshot 2, me, lastIncludedIndex, log[0].Index, lastIndex = ",rf.me,args.LastIncludedIndex, rf.log[0].Index,rf.getLastIndex())

	defer rf.persist()

	if rf.currentTerm > args.Term{
		reply.Term=rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
	}
	reply.Term = rf.currentTerm

	rf.chanHeartBeat <- true

	// if commitIndex > args.LastIncludedIndex, ignore
	/*
	   Think about this: leader sends log to a follower with commitIndex N, follower appends log and commits up to N, then reply;
	   However, reply lost, so leader doesn't update nextIndex[]. Later on, leader sends a snapshot to this follower with lastIncludedIndex M, while M < N;
	   If follower accepts this snapshot and update commitIndex(N->M), then the log entries whose index are between M and N will be applied(notified) twice,
	   which may cause deadlock if service's design is fragile, i.e. service using channel to block-reading, and write the result into another channel A without
           process reading from A, and raft is blocked in applied-process.
	*/
	if rf.commitIndex > args.LastIncludedIndex{
		reply.NextIndex = rf.commitIndex+1
		return
	}	
	
	// create new snapshot? we don't need to wait for more data chunks cause we send all at once
	// save snapshot file	
	rf.persister.SaveSnapshot(args.Data)

	// If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply.
	// Otherwise, discard the entire log
	// truncate log
	rf.truncateLog(args.LastIncludedIndex,args.LastIncludedTerm) 
 
	//fmt.Println("in InstallSnapshot after truncateLog, me, lastIncludedIndex, log[0].Index, lastIndex = ",rf.me,args.LastIncludedIndex,rf.log[0].Index,rf.getLastIndex())

	// update commitIndex and lastApplied  
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	// Reset state machine using snapshot contents
	var lastIncludedIndex int
	var lastIncludedTerm int
	var data []byte
	r := bytes.NewBuffer(args.Data)
	d := gob.NewDecoder(r)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)
	d.Decode(&data)

	var m ApplyMsg
	m.UseSnapshot = true
	m.Snapshot = data
	rf.chanApplyMsg <- m

	reply.NextIndex = args.LastIncludedIndex + 1
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()	
	defer rf.mu.Unlock()	
	if ok{
		if rf.state!=STATE_LEADER{ 
			return ok
		}
		if args.Term!=rf.currentTerm{ 
			return ok
		}
		if reply.Term>rf.currentTerm{
			rf.currentTerm=reply.Term
			rf.state=STATE_FOLLOWER
			rf.persist()
			return ok
		}
		// update nextIndex and matchIndex 
		//rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.nextIndex[server] = reply.NextIndex
		rf.matchIndex[server] = rf.nextIndex[server]-1	
		//fmt.Println("in sendSnapshot call true, me, nextIndex, lastIncludedIndex = ",server,rf.nextIndex[server],args.LastIncludedIndex)
	}else{
		//fmt.Println("in sendSnapshot call false, me, nextIndex, lastIncludedIndex = ",server,rf.nextIndex[server],args.LastIncludedIndex)
	}
	return ok
}


func (rf *Raft) StartSnapshot(data []byte,lastIncludedIndex int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//attention! not only leader can reply, every server is necessary to trim its log
	//if rf.state!=STATE_LEADER{
	//	return false
	//}
	smallestIndex := rf.log[0].Index
	
	//attention! if smallestIndex >= lastIncludedIndex, ingore 
	if smallestIndex >= lastIncludedIndex{
		return true
	}
	// check bound
	if lastIncludedIndex>rf.getLastIndex(){	
		//fmt.Println("in StartSnapshot, me, lastIncludedIndex, rf.getLastIndex() = ",rf.me,lastIncludedIndex,rf.getLastIndex())
		return false
	}
	
	//fmt.Println("in StartSnapshot, me, isLeader, smallestIndex, lastIncludedIndex, log size = ",rf.me,rf.state==STATE_LEADER, smallestIndex,lastIncludedIndex,len(rf.log))
	lastIncludedTerm := rf.log[lastIncludedIndex-smallestIndex].Term
	//truncate log
	rf.truncateLog(lastIncludedIndex,lastIncludedTerm) 

	//save currrent snapshot
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(lastIncludedIndex) 
	e.Encode(lastIncludedTerm)
	e.Encode(data)
	ss := w.Bytes()
	rf.persister.SaveSnapshot(ss)
	//fmt.Println("in StartSnapshot, me, snapshot size = ",rf.me,rf.persister.RaftStateSize())
	//persist
	rf.persist()
	return true
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state!=STATE_LEADER{
		isLeader = false
		return index,term,isLeader
	}

	// append command to log
	var newLog LogEntry
	newLog.Index = rf.getLastIndex()+1
	newLog.Command = command
	newLog.Term = rf.currentTerm
	rf.log = append(rf.log,newLog)

	//fmt.Println("in Start, me, newLog.Index, len = ",rf.me,newLog.Index,len(rf.log))
	rf.persist()

	index = newLog.Index
	term = newLog.Term
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.chanLeader = make(chan bool,1)
	rf.chanHeartBeat = make(chan bool,1)
	rf.chanSuccVote = make(chan bool,1)
	rf.chanApplyMsg = applyCh
	rf.chanCommit = make(chan bool,1)

	rf.state = STATE_FOLLOWER
	rf.currentTerm = 0
	rf.voteCount = -1
	rf.log = append(rf.log,LogEntry{Term:0,Index:0})
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// initialize from snapshot before a crash
	rf.readSnapshot(persister.ReadSnapshot())

	go func(){
		for {
			switch rf.state {
			case STATE_FOLLOWER:
				select {
				case <- rf.chanSuccVote:
				case <- rf.chanHeartBeat:
				case <- time.After(time.Duration(rand.Int63()%150+150)*time.Millisecond):
					rf.mu.Lock()
					rf.state = STATE_CANDIDATE
					rf.mu.Unlock()
				}
			case STATE_CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor=me
				rf.voteCount=1 
				rf.mu.Unlock()
				go rf.broadcastRequestVote();

				select{
				case <- rf.chanLeader:
					rf.mu.Lock()
					rf.state=STATE_LEADER 
					//now server is leader,so it should reinitialize nextIndex and matchIndex 
					rf.nextIndex = make([]int,len(rf.peers))
					rf.matchIndex = make([]int,len(rf.peers))
					for i:=0;i<len(rf.peers);i++{
						rf.nextIndex[i] = rf.getLastIndex()+1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				case <- rf.chanHeartBeat:
				case <- rf.chanSuccVote:
				case <- time.After(time.Duration(rand.Int63()%150+150)*time.Millisecond):
					//still candidate 
				}
			case STATE_LEADER: 
				select{
				case <- rf.chanSuccVote:
				case <- time.After(HEARTBEATINTERVAL):
					rf.broadcastAppendEntries()
				}
			} 
		}
	}()

	// wait for commited-command, that means we should send apply to client
	go func(){
		for{
			select{
			case <- rf.chanCommit:
				rf.mu.Lock()		
				smallestIndex := rf.log[0].Index
				if rf.lastApplied+1 >= smallestIndex {
					for i:=rf.lastApplied+1;i<=rf.commitIndex;i++{
						var apply ApplyMsg
						apply.Index = i
						apply.Command = rf.log[i-smallestIndex].Command
						rf.lastApplied = i
						//fmt.Println("in raft's chanCommit,me,isLeader,log[0].Index,commitIndex,index,command=",rf.me,rf.state==STATE_LEADER,rf.log[0].Index,rf.commitIndex,i,apply.Command)  
						rf.chanApplyMsg <- apply 
					}
				}		
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
