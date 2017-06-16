package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
//	"fmt"
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
	Key string
	Value string
	Op string
	ClerkId int64
	ReqId	int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	rpcResult map[int]chan Op // used for waiting raft's apply
	kvMap 	map[string]string // used for storing key-value pair
	ackMap	map[int64]int64   // used for identifing the repeated request	
}

func (kv *RaftKV) checkRepeatedRequest(args Op)(isRepeated bool) {
	//kv.mu.Lock() 
 	//defer kv.mu.Unlock() 
 
	if maxAck,ok := kv.ackMap[args.ClerkId];ok{ 
		if args.ReqId <= maxAck {
			//fmt.Println("in checkRepeatedRequest, me, maxAck, reqId, reject op =",kv.me,maxAck,args.ReqId,args)
		}
		return args.ReqId <= maxAck
	}else{
		return false
	}
}

func (kv *RaftKV) storePair(args Op) {
	//kv.mu.Lock() 
 	//defer kv.mu.Unlock() 
 
	switch args.Op{
	case "Put":
		kv.kvMap[args.Key] = args.Value 
	case "Append":
		kv.kvMap[args.Key] += args.Value
	}	
	//maxAck,ok := kv.ackMap[args.ClerkId]
	//fmt.Println("in storePair, me, kv.ackMap, ok, old maxAck, reqId, accept op =",kv.me,kv.ackMap,ok,maxAck,args.ReqId,args)
	//store the max reqId
	kv.ackMap[args.ClerkId] = args.ReqId 
}

func checkNewApply(newApply Op, oldArgs Op) bool{
	return newApply == oldArgs
	/*
	return newApply.Key == oldArgs.Key &&
		newApply.Value == oldArgs.Value &&
		newApply.Op == oldArgs.Op &&
		newApply.ClerkId == oldArgs.ClerkId &&
		newApply.ReqId == oldArgs.ReqId 
	*/
}

func (kv *RaftKV) callRaft(args Op)(wrongLeader bool) {
	//fmt.Println("in callRaft,me,command=",kv.me,args) 
	index,_,isLeader := kv.rf.Start(args)
	if !isLeader{ 
		return true
	}
	
	//fmt.Println("in callRaft,isLeader,me,index,command=",isLeader,kv.me,index,args) 
	kv.mu.Lock()  
	if _,ok := kv.rpcResult[index];!ok{  
		kv.rpcResult[index] = make(chan Op,1)
	}
	ch := kv.rpcResult[index] 
	kv.mu.Unlock() //unlock before reading from channel, or may cause deadlock

	// wait for notification
	// raft may change leader while kvServer is waiting for notification, even though server is waiting for the index to be committed, not the leader,
	// it may cause some problem if kvServer simply blocks to wait without timeout.
	// i.e. raft leader accepts some command, and crashes, then a new leader is elected. kvServer simply blocks to wait without timeout, no longer send new command.
	// However, raft won't commit old log until it accepts new command and append to a majority of servers. 
	// This causes a deadlock.
 
	// you can simply wait like the following, if timeout, simply return wrongLeader and clerk will resend request.
	/*
	select{
	case <- ch:
		return op == args
	case <- time.After(100 * time.Millisecond):
		return true
	} 
	*/

	// read raft state. if not leader, return
	// args may be different from kv.rpcResult[index].
	// i.e. At the beginning, kvServer A is the leader, it accept a request(log index is index1), and waits for raft's commit. 
	// Unfortunately, network partition happens.
	// kvServer A is now in the minority partition, a new raft leader in the majority partition is elected,
	// but kvServer A still blocks in select-case and still within the timeout period.
	// In the meantime, the new leader accepts new commands and commit. And then, the partition heals.
	// Now kvServer A may accept the leader's log entry at index1, which is different from the old request that kvServer A is waiting for .

	for{
		select{
		case op := <- ch:
			//fmt.Println("in callRaft,index,new log entry,old request=",index,op,args) 
			//fmt.Println("in callRaft,me,index,if same,args =",kv.me,index,op==args,args) 
			return !checkNewApply(op,args)
		case <- time.After(20 * time.Millisecond):
			_, isLeader := kv.rf.GetState()
			if !isLeader{
				return true
			}
		}
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) { 
	//fmt.Println("in Get,me,args =",kv.me,args) 
	// Your code here.
	// A kvraft should not complete a Get() RPC if it is not part of a majority(so that it does not serve stale data).
	// A simple solution is to enter every Get() in the Raft log(the request will not be committed if it is not part of a majority).

	// Since a kvraft should not complete a Get() RPC if it is not part of a majority, 
	// we don't check repeated request before callRaft()

	var op Op
	op.Key = args.Key 
	op.Op = "Get"
	op.ClerkId = args.ClerkId
	op.ReqId = args.ReqId

	//append to log
	wrongLeader := kv.callRaft(op) 
	if wrongLeader{
		reply.WrongLeader = true
		return
	}
  	//reply to clerk
	reply.WrongLeader = false
	kv.mu.Lock() 
	if _,ok:=kv.kvMap[args.Key];ok{
		reply.Err = OK
		reply.Value = kv.kvMap[args.Key]
	}else{
		reply.Err = ErrNoKey
		reply.Value = ""
	}	
	kv.mu.Unlock()
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//fmt.Println("in PutAppend,me,args =",kv.me,args) 
	// Your code here.
	// Since a kvraft should not complete a PutAppend() RPC if it is not part of a majority, 
	// we don't check repeated request before callRaft()

	var op Op
	op.Key = args.Key
	op.Value = args.Value
	op.Op = args.Op 
	op.ClerkId = args.ClerkId
	op.ReqId = args.ReqId

	//append to log
	wrongLeader := kv.callRaft(op) 
	if wrongLeader{
		reply.WrongLeader = true
		return
	}
 	//reply to clerk
	reply.WrongLeader = false
	reply.Err = OK
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
 
	kv.rpcResult = make(map[int]chan Op)
	kv.kvMap = make(map[string]string)
	kv.ackMap = make(map[int64]int64)
	//fmt.Println("StartKVServer")
	go func() {
		for m := range kv.applyCh {  
			//do not call raft's function who holds raft's lock here,like kv.rf.GetState(), may cause deadlock.
			//i.e. raft holds its lock, and tries to write into ApplyMsg channel whenever it has new commit,
			//if we call kv.rf.GetState() before reading from ApplyMsg channel,
			//kv.rf.GetState() will block in tring to acquire raft's lock, but raft will not unlock until we read from ApplyMsg channel
			
			if m.UseSnapshot {
				kv.mu.Lock() 
				r := bytes.NewBuffer(m.Snapshot)
				d := gob.NewDecoder(r)
				d.Decode(&kv.kvMap)
				d.Decode(&kv.ackMap)
				kv.mu.Unlock()
			} else {
				op := m.Command.(Op)
				kv.mu.Lock() 
				// checkRepeatedRequest() and storePair() must be execed within the same lock.
				// Otherwise, server may store the same repeated request twice: after the firt checkRepeatedRequest(), 
				// and before the first storePair(), call checkRepeatedRequest() again, the result is without repetition.
				// deal with repeated request
				if isRepeated := kv.checkRepeatedRequest(op);!isRepeated{ 
					// store the key-value pair
					kv.storePair(op)
				}	

				// to notify
				if _,ok:=kv.rpcResult[m.Index];!ok{
					kv.rpcResult[m.Index] = make(chan Op,1)
				} 
				ch := kv.rpcResult[m.Index]
				kv.mu.Unlock()// unlock before write into channel, or may cause deadlock

				ch <- op // write into every server, but only read from leader
				//fmt.Println("in applyCh,index,me,clerkId,reqId,op=",m.Index,kv.me,op.ClerkId,op.ReqId,op)

				// need to save a snapshot
				if maxraftstate!=-1 && maxraftstate<=persister.RaftStateSize() {
					// save a snapshot
					kv.mu.Lock()
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.kvMap)
					e.Encode(kv.ackMap)
					data := w.Bytes()
					// persister.SaveSnapshot(data)
					// tell the Raft library that it has snapshotted, so that Raft can discard old log entries
					// do not call raft's function who holds raft's lock here,like kv.rf.StartSnapshot(), may cause deadlock
					go kv.rf.StartSnapshot(data,m.Index)
					kv.mu.Unlock()
				}
			}   
		}
	}()
	return kv
}
 
