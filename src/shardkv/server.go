package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "time"
import "bytes"
import "fmt"
import "strconv"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Op string
	ClerkId interface{}
	ReqId	int64
	Config shardmaster.Config
	Shard int
}

const (
	Normal          = "Normal"
	Lose      	= "Lose"
	Gain 		= "Gain"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	serverId int64
	serverId_config int64
	serverId_shard int64
	reqId	int64
	sm      *shardmaster.Clerk // connect to shardmaster
	doneMigration chan bool // if done the migrate
	config shardmaster.Config
	migrateShard map[int]bool
	rpcResult map[int]chan Op // used for waiting raft's apply
	kvMap 	map[int]map[string]string // used for storing shard:key-value pair
	ackMap	map[interface{}]int64   // used for identifing the repeated request	
}


func (kv *ShardKV) checkRepeatedRequest(args Op)(isRepeated bool) {
	//kv.mu.Lock() 
 	//defer kv.mu.Unlock() 
 
	if maxAck,ok := kv.ackMap[args.ClerkId];ok{ 
		if args.ReqId <= maxAck {
			//fmt.Println("in checkRepeatedRequest, me, maxAck, reqId, reject op =",kv.me,maxAck,args.ReqId,args)
			fmt.Println("reject op, me, group, config.Num =",kv.me,kv.gid,args)
		}
		return args.ReqId <= maxAck
	}else{
		return false
	}
}

func (kv *ShardKV) storePair(args Op) {
	//kv.mu.Lock() 
 	//defer kv.mu.Unlock() 
 
	shard := key2shard(args.Key)
	switch args.Op{
	case "Put":
		if _,ok:=kv.kvMap[shard];!ok{
			kv.kvMap[shard] = make(map[string]string)
		}
		kv.kvMap[shard][args.Key] = args.Value 
	case "Append":
		if _,ok:=kv.kvMap[shard];!ok{
			kv.kvMap[shard] = make(map[string]string)
		}
		kv.kvMap[shard][args.Key] += args.Value
	case "Config":
		//go func(config shardmaster.Config) {
		//	kv.mu.Lock()
		//	defer kv.mu.Unlock()
			if args.Config.Num > kv.config.Num {
				kv.config = args.Config
			}
			//fmt.Println("in storePair, me, group, config.Num =",kv.me,kv.gid,args.Config.Num)
		//}(args.Config)
	case "NewShard":
		//try to update config if server already gain all shard in this config
		newShards := make(map[int]bool)
		for s,g := range args.Config.Shards {
			if g==kv.gid {
				newShards[s] = true
			}
		}
		ready := true

		//set flag, server has already gain the shard
		kv.migrateShard[args.Shard] = true
		curShards := make(map[int]bool)
		for s,g := range kv.config.Shards {
			if g==kv.gid {
				curShards[s] = true
			}
		}
		for s,_:= range newShards{
			if _,ok:=curShards[s];!ok{
				//it is a new shard, check if already gain data
				if value,_:=kv.migrateShard[s];!value{
					ready = false
					break
				}
			}
		}
		fmt.Println("New Shard, server,group,kv.migrateShard,config.shards = ",kv.me,kv.gid,kv.migrateShard,args.Config.Shards)

		//fmt.Println("In Migrate, server,group,shard,kvs=",kv.me,kv.gid,args.Shard,kv.kvMap[args.Shard])
		if ready {
			/*			
			for n := kv.config.Num + 1; n <= args.Config.Num; n++ {  
				go func(configNum int){
					var op Op
					op.ClerkId = kv.serverId_config
					op.ReqId = int64(configNum) 
					op.Op = "Config" 
					op.Config = kv.sm.Query(configNum) 
					kv.callRaft(op) 
				}(n)
			}
			*/
			go func(config shardmaster.Config){
				for{
					var op Op
					op.ClerkId = kv.serverId_config
					op.ReqId = int64(config.Num) 
					op.Op = "Config" 
					op.Config = config
					wrongLeader := kv.callRaft(op) 
					if !wrongLeader{
						fmt.Println("New Shard ready, server,group,kv.config.Num,args.ConfigNum = ",kv.me,kv.gid,kv.config.Num,args.Config.Num)
						return
					}
				}
			}(args.Config)
			for k,_ := range kv.migrateShard{
				kv.migrateShard[k] = false
			}
		}
	}
	//maxAck,ok := kv.ackMap[args.ClerkId]
	//fmt.Println("in storePair, me, kv.ackMap, ok, old maxAck, reqId, accept op =",kv.me,kv.ackMap,ok,maxAck,args.ReqId,args)
	//store the max reqId
	kv.ackMap[args.ClerkId] = args.ReqId 
}

func checkNewApply(newApply Op, oldArgs Op) bool{
	if newApply.Op == oldArgs.Op {
		if newApply.Op == "Config" {
			return newApply.ReqId == oldArgs.ReqId
		}
		return newApply.ClerkId == oldArgs.ClerkId && newApply.ReqId == oldArgs.ReqId
	}
	return false 
}

func (kv *ShardKV) callRaft(args Op)(wrongLeader bool) {
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op
	op.Key = args.Key 
	op.Op = "Get"
	op.ClerkId = args.ClerkId
	op.ReqId = args.ReqId

	//if ok to server the request 
	//kv.mu.Lock()
	lastConfig := kv.sm.Query(-1)
	for lastConfig.Num != kv.config.Num {
		//waiting
		//fmt.Println("In Get, waiting")
		//kv.mu.Unlock()
		time.Sleep(50)
		lastConfig = kv.sm.Query(-1)
		//kv.mu.Lock()
	}
	if kv.gid != kv.config.Shards[key2shard(args.Key)] {
		reply.Err = ErrWrongGroup
		//kv.mu.Unlock()
		return
	}
	//kv.mu.Unlock() 

	//append to log
	wrongLeader := kv.callRaft(op) 
	if wrongLeader{
		reply.WrongLeader = true
		return
	}
  	//reply to clerk
	reply.WrongLeader = false
	kv.mu.Lock() 
	shard := key2shard(args.Key)
	if _,ok:=kv.kvMap[shard];ok{
		if _,ok:=kv.kvMap[shard][args.Key];ok{
			reply.Err = OK
			reply.Value = kv.kvMap[shard][args.Key]
		}else{
			reply.Err = ErrNoKey
			reply.Value = ""
		}
	}else{
		reply.Err = ErrNoKey
		reply.Value = ""
	}	
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op
	op.Key = args.Key
	op.Value = args.Value
	op.Op = args.Op 
	op.ClerkId = args.ClerkId
	op.ReqId = args.ReqId

	//if ok to server the request 
	//kv.mu.Lock()
	lastConfig := kv.sm.Query(-1)
	for lastConfig.Num != kv.config.Num {
		//waiting
		//fmt.Println("In PutAppend, waiting")
		//kv.mu.Unlock()
		time.Sleep(50)
		lastConfig = kv.sm.Query(-1)
		//kv.mu.Lock()
	}
	if kv.gid != kv.config.Shards[key2shard(args.Key)] {
		reply.Err = ErrWrongGroup
	//	kv.mu.Unlock()
		return
	}
	//kv.mu.Unlock() 

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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

type MigrateArgs struct {
	SourceServerId interface{}
	Shard int
	Data  []byte
	BaseReqId int64
	ConfigNum int
}

type MigrateReply struct {
	WrongLeader bool
	Err         Err
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply){
	curConfigNum := kv.config.Num
	//fmt.Println("before Migrate,server,group,kv.config.Num,args.ConfigNum=",kv.me,kv.gid,kv.config.Num,args.ConfigNum)
	for {
		if curConfigNum == args.ConfigNum-1 {
			break
		}else if curConfigNum > args.ConfigNum-1{
			reply.Err = ErrRepeatedMigration
			return
		}else{
			reply.Err = ErrNotReady
			return
		}
	}

	//do not hold lock
	kvs := make(map[string]string)
	r := bytes.NewBuffer(args.Data)
	d := gob.NewDecoder(r)
	d.Decode(&kvs)
	//fmt.Println("before Migrate,server,group,shard,kvs=",kv.me,kv.gid,args.Shard,kvs)
	//need to put into log
	_,isLeader := kv.rf.GetState()
	if !isLeader{
		reply.WrongLeader = true
		return
	}	

	var index int64 = 0
	for k,v:=range kvs{
		var op Op
		op.Key = k
		op.Value = v
		op.Op = "Put"
		op.ClerkId = args.SourceServerId
		op.ReqId = args.BaseReqId + index
		index++
		//if leader changes during callRaft, return and source-server will try to send to the new leader again 
		wrongLeader := kv.callRaft(op) 
		if wrongLeader{
			reply.WrongLeader = true
			return
		}
	}
 
	var op Op
	//make it unique
	op.ClerkId = strconv.FormatInt(kv.serverId_shard,10) + "_" + strconv.Itoa(args.ConfigNum) + "_" + strconv.Itoa(args.Shard)  
	op.ReqId = 0 
	op.Op = "NewShard" 
	op.Config = kv.sm.Query(args.ConfigNum)
	op.Shard = args.Shard
	wrongLeader := kv.callRaft(op) 
	if wrongLeader{
		reply.WrongLeader = true
		return
	}
	fmt.Println("In Migrate,server,group,shard,args.kvs,kvs =",kv.me,kv.gid,args.Shard,kvs,kv.kvMap[args.Shard])
	reply.Err = OK
}


func (kv *ShardKV) migrateData(shard int, configNum int, servers []string, done chan bool){
	kv.mu.Lock() 
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.kvMap[shard])
	data := w.Bytes()
	kv.mu.Unlock() 
	for {
		//only need to send data to leader
		for _,server:=range servers {
			for{
				_,isLeader := kv.rf.GetState()
				if !isLeader {
					done <- true
					return
				}
			
				var req MigrateArgs
				//cause we migrate many shards concurrently, different shards may have the same reqId, so we make ClerkId different by Shard number
				req.SourceServerId = strconv.FormatInt(kv.serverId_shard,10) + "_" + strconv.Itoa(shard) 
				req.Shard = shard
				req.Data = data
				req.BaseReqId = kv.reqId
				req.ConfigNum = configNum
				//fmt.Println("migrateData, server,group,target,shard,data = ",kv.me,kv.gid,server,shard,kv.kvMap[shard])

				var reply MigrateReply
				srv := kv.make_end(server)
				ok := srv.Call("ShardKV.Migrate", &req, &reply)
				if !ok {
					break
				}
				if ok && reply.WrongLeader{
					break
				}
				if ok && reply.Err == OK {
					goto OUT
				}
				if ok && reply.Err == ErrRepeatedMigration {
					goto OUT1
				}
				if ok && reply.Err == ErrNotReady {
					time.Sleep(50)
				}
			}
		}
	}
OUT:
	kv.mu.Lock() 
	kv.reqId += (int64)(len(kv.kvMap[shard]))
	kv.mu.Unlock() 
OUT1:
	done <- true
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.sm = shardmaster.MakeClerk(masters)
	//kv.shardChange = make(chan string)
	//kv.keyToShard = make(chan int)
	//kv.doneMigration = make(chan bool)
	//kv.shards = make(map[int]bool)
	kv.migrateShard = make(map[int]bool,shardmaster.NShards)
	kv.serverId = nrand()
	kv.serverId_config = nrand()
	kv.serverId_shard = nrand()
	kv.reqId = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.rpcResult = make(map[int]chan Op)
	kv.kvMap = make(map[int]map[string]string)
	kv.ackMap = make(map[interface{}]int64)
	fmt.Println("StartKVServer")
	
	//cause Get and PutAppend function will block first by testing if can server request, we should send new request to raft for recovering old log
	go func(){
		var op Op
		op.ClerkId = kv.serverId
		op.ReqId = int64(0) 
		op.Op = "Boot" 
		for{
			wrongLeader := kv.callRaft(op) 
			if wrongLeader{
				time.Sleep(50)
			}else{
				break
			}
		}
	}()
	
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
				d.Decode(&kv.config)
				d.Decode(&kv.reqId)
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
				//fmt.Println("in applyCh,index,me,gid,reqId,op =",m.Index,kv.me,kv.gid,op.ReqId,op.Op)

				// need to save a snapshot
				if maxraftstate!=-1 && maxraftstate<=persister.RaftStateSize() {
					// save a snapshot
					kv.mu.Lock()
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.kvMap)
					e.Encode(kv.ackMap)
					e.Encode(kv.config)
					e.Encode(kv.reqId)
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

	//tackle the problem of configuration changes
	go func(){ 
		for{
			_,isLeader := kv.rf.GetState()
			if isLeader {
				lastConfig := kv.sm.Query(-1)
				//no group join
				if lastConfig.Num == 0{
					//no group join, we can not server the request
					//kv.shardChange <- Lose
					continue
				}

				curShards := make(map[int]bool)
				kv.mu.Lock() 
				for s,g := range kv.config.Shards {
					if g==kv.gid {
						curShards[s] = true
					}
				}
				curConfigNum := kv.config.Num
				kv.mu.Unlock()
				for n := curConfigNum + 1; n <= lastConfig.Num; n++ {
					config := kv.sm.Query(n)
					newShards := make(map[int]bool)
					for s,g := range config.Shards {
						if g==kv.gid {
							newShards[s] = true
						}
					}
					//if gain
					gainShards := make([]int,0)
					for s,_:= range newShards{
						if _,ok:=curShards[s];!ok{
							gainShards = append(gainShards,s)
						}
					}
					//if lose
					lostShards := make([]int,0)
					for s,_:= range curShards{
						if _,ok:=newShards[s];!ok{	
							lostShards = append(lostShards,s)
						}
					}

					// if configNum = 1, it means that we should update config, if not, Get and PutAppend may block by testing if can server request
					// especially if only one group
					if n == 1 {
						fmt.Println("first config,server,group,config.shards = ",kv.me,kv.gid,config.Shards)
						//try to update config
						op := Op{
							ClerkId:	kv.serverId_config,
							ReqId:  	int64(n),
							Op: 		"Config",
							Config:		config,
						}
						kv.callRaft(op) 
						continue
					} 
 					if len(gainShards)==0 && len(lostShards)==0{
						//try to update config
						op := Op{
							ClerkId:	kv.serverId_config,
							ReqId:  	int64(n),
							Op: 		"Config",
							Config:		config,
						}
						kv.callRaft(op) 
						continue
					}
if kv.gid==101{
//fmt.Println(kv.me,n,curShards,gainShards,lostShards)
}
					if len(gainShards)>0 {
						for _,s:=range gainShards{
							curShards[s] = true
						}
					}
					if len(lostShards)>0 {
						//Before migrateData, we should wait the last gain(if has).
						//i.e. if kv.config.Num = n-1 : ok to migrate data
						//if kv.config.Num > n-1 : no need to migrate data
						needToMigrate := false
						for {
							if kv.config.Num == n-1 {
								needToMigrate = true
								break
							}else if kv.config.Num > n-1{
								needToMigrate = false
								break
							}
							time.Sleep(50)
						}
						if needToMigrate{
							fmt.Println("will lose shard,server,group,lostShards,kv.config.shards,config.shards = ",kv.me,kv.gid,lostShards,kv.config.Shards,config.Shards)
							done := make(chan bool,len(lostShards))
							for _,s:=range lostShards{
								delete(curShards,s)
								//If a replica group loses a shard, it must stop serving requests to keys in that shard immediately, 
								//and start migrating the data for that shard to the replica group that is taking over ownership.
								//only the leader of the servers in group need to send data
								//fmt.Println("try migrateData,server,group,s,config.shards = ",kv.me,kv.gid,s,config.Shards)
								go kv.migrateData(s,n,config.Groups[config.Shards[s]],done)
							}
							for i:=0;i<len(lostShards);i++{
								<- done  
							}
							fmt.Println("done migrateData,server,group,config.Num = ",kv.me,kv.gid,n)

							//try to update config
							op := Op{
								ClerkId:	kv.serverId_config,
								ReqId:  	int64(n),
								Op: 		"Config",
								Config:		config,
							}
							kv.callRaft(op) 
						}
						
					}
				}
			}
			//fmt.Println(kv.me,kv.gid,config.Groups[kv.gid]) 
			time.Sleep(100 * time.Millisecond)
		} 
	}()

	return kv
}
