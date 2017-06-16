package raftkv

import (
	"labrpc"
	"crypto/rand"
	"math/big"
	"sync" 
//	"fmt"
//	"time"
)  

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int	
	reqId	int64
	clerkId	int64
	mu      sync.Mutex
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
	ck.lastLeader = 0
	ck.reqId = 0
	ck.clerkId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string { 
	// You will have to modify this function. 

	var args GetArgs
	args.Key = key 		
	args.ClerkId = ck.clerkId
	ck.mu.Lock()
	args.ReqId = ck.reqId
	ck.reqId++	
	ck.mu.Unlock()
 
	nServer := len(ck.servers) 
	retValue := "" 
	for{
		for i:=0;i<nServer;i++{ 			
			var reply GetReply //reply should declared within the for-loop, otherwise we cann't get the correct value in time

			ck.mu.Lock()
			leaderIndex := (i+ck.lastLeader)%len(ck.servers)  
			ck.mu.Unlock()
			//fmt.Println("in (ck *Clerk) Get,leaderIndex,offest,args=",leaderIndex,i,args) 
			ok := ck.servers[leaderIndex].Call("RaftKV.Get", &args, &reply) 
			if ok && reply.WrongLeader==false{   
				//servers[] is different in each Clerk.
				//i.e. leaderIndex is different in each Clerk.
				//fmt.Println("in (ck *Clerk) Get,leader server,leaderIndex,offest=",ck.servers[leaderIndex],leaderIndex,i) 
				ck.mu.Lock()
				ck.lastLeader = leaderIndex //may lead to bad influence, if has concurrent calls
				ck.mu.Unlock()
				
				if reply.Err == OK{
					retValue = reply.Value
				}
				return retValue
			}
		} 
		//time.Sleep(500*time.Millisecond)
	}  
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.  

	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op 	
	args.ClerkId = ck.clerkId

	ck.mu.Lock()
	args.ReqId = ck.reqId
	ck.reqId++	
	ck.mu.Unlock() 

	nServer := len(ck.servers) 
	for{
		for i:=0;i<nServer;i++{ 
			var reply PutAppendReply//reply should declared within the for-loop, otherwise we cann't get the correct value in time
	
			ck.mu.Lock()
			leaderIndex := (i+ck.lastLeader)%len(ck.servers)  
			ck.mu.Unlock()
			//fmt.Println("in (ck *Clerk) PutAppend,leaderIndex,offest,args=",leaderIndex,i,args) 
			ok := ck.servers[leaderIndex].Call("RaftKV.PutAppend", &args, &reply)	
			//Unreliable network may cause Call to return false, then we will try to call another server, 
			//even thouth current server is related with the leader. But we can not distinguish such situation.
			if ok && reply.WrongLeader==false{   
				//servers[] is different in each Clerk. 
				//i.e. leaderIndex is different in each Clerk.
				//fmt.Println("in (ck *Clerk) PutAppend,leader server,leaderIndex,offest=",ck.servers[leaderIndex],leaderIndex,i) 
				ck.mu.Lock()
				ck.lastLeader = leaderIndex //may lead to bad influence, if has concurrent calls
				ck.mu.Unlock()
				
				return
			}  
		} 
		//time.Sleep(500*time.Millisecond)
	}  
}

func (ck *Clerk) Put(key string, value string) { 
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
