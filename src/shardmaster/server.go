package shardmaster


import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "time"
//import "fmt"
import "sort"

const (
	JOIN  = "Join" 
	LEAVE = "Leave" 
	MOVE  = "Move" 
	QUERY = "Query" 
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	rpcResult map[int]chan Op // used for waiting raft's apply
	ackMap	map[int64]int64   // used for identifing the repeated request	

	groupToShardsSlice GroupToShardsSlice
}

/*
func Sort(data Interface) {
    // Switch to heapsort if depth of 2*ceil(lg(n+1)) is reached.
    n := data.Len()
    maxDepth := 0
    for i := n; i > 0; i >>= 1 {
        maxDepth++
    }
    maxDepth *= 2
    quickSort(data, 0, n, maxDepth)
}

type Interface interface {
    // Len is the number of elements in the collection.
    Len() int
    // Less reports whether the element with
    // index i should sort before the element with index j.
    Less(i, j int) bool
    // Swap swaps the elements with indexes i and j.
    Swap(i, j int)
}
*/

//extended struct
type GroupToShards struct {
	groupId int
	shardsNum int
	shards []int
}
 
//GroupToShardsSlice can be sorted 
type GroupToShardsSlice []GroupToShards 
func (p GroupToShardsSlice) Len() int           { return len(p) }
//sort desc
func (p GroupToShardsSlice) Less(i, j int) bool { return p[i].shardsNum > p[j].shardsNum }
func (p GroupToShardsSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type Op struct {
	// Your data here.
	Op string
	ClerkId int64
	ReqId	int64
	//Join
	Servers map[int][]string // new GID -> servers mappings
	//Leave
	GIDs []int
	//Move
	Shard int
	GID   int
	//Query
	Num int // desired config number
}

func (sm *ShardMaster) checkRepeatedRequest(args Op)(isRepeated bool) {
	if maxAck,ok := sm.ackMap[args.ClerkId];ok{ 
		if args.ReqId <= maxAck {
			//fmt.Println("in checkRepeatedRequest, me, maxAck, reqId, reject op =",sm.me,maxAck,args.ReqId,args)
		}
		return args.ReqId <= maxAck
	}else{
		return false
	}
}

func (sm *ShardMaster) getLastConfigNum() int{
	return sm.configs[len(sm.configs)-1].Num
}

func (sm *ShardMaster) store(args Op) {
	switch args.Op{
	case JOIN:
		// new config
		var config Config
		config.Num = sm.getLastConfigNum()+1
		copy(config.Shards[0:],sm.configs[config.Num-1].Shards[0:])
		config.Groups = make(map[int][]string)
		//copy from old config: create a new map object (with make()) and copy the keys and values individually
		oldGroups := sm.configs[config.Num-1].Groups
		for gid,_:=range oldGroups{
			config.Groups[gid] = make([]string,len(oldGroups[gid]))
			copy(config.Groups[gid],oldGroups[gid])
		}
		//add new group if new
		newGroupToShardsSlice := make(GroupToShardsSlice,0)
		for gid,_:=range args.Servers{
			if _,ok:=config.Groups[gid];!ok{
				config.Groups[gid] = make([]string,0)
				config.Groups[gid] = append(config.Groups[gid],args.Servers[gid]...)
				//prepare for new GroupToShardsSlice
				var groupToShards GroupToShards
				groupToShards.groupId = gid
				groupToShards.shardsNum = 0
				groupToShards.shards = make([]int,0)
				newGroupToShardsSlice = append(newGroupToShardsSlice,groupToShards)
			}
		}
		//fmt.Println("in join: ",args.ClerkId,args.ReqId,config.Groups,oldGroups)

		//shard divide
		groupNum := len(config.Groups)
		aveShardsNum := NShards/groupNum
		//up to NShards
		if aveShardsNum==0 {
			groupNum = NShards
			aveShardsNum = 1
		}
		var gids []int
		for k := range config.Groups {
			gids = append(gids, k)
		}
		//need to divide: new group and current is no full
		if len(newGroupToShardsSlice)>0 && len(sm.groupToShardsSlice)<NShards {
			//is the first join
			if len(sm.groupToShardsSlice)==0 {
				//count each group's shardsNum
				for i:=0;i<groupNum;i++{
					newGroupToShardsSlice[i].shardsNum = aveShardsNum
				}
				rest := NShards%groupNum
				for i:=0;i<rest;i++{
					newGroupToShardsSlice[i].shardsNum += 1
				}
				//fill config.Shards and new GroupToShardsSlice's shards
				shardIndex:=0
				for i:=0;i<groupNum;i++{
					//loop shardsNum times
					for j:=0;j<newGroupToShardsSlice[i].shardsNum;j++{
						newGroupToShardsSlice[i].shards = append(newGroupToShardsSlice[i].shards,shardIndex)
						config.Shards[shardIndex] = newGroupToShardsSlice[i].groupId
						shardIndex++
					}
				}			
			}else{
				//sort groups by its shardsNum
				sort.Sort(sm.groupToShardsSlice)
				//how many group may be added 
				newGroupsCount := len(newGroupToShardsSlice)
				if newGroupsCount+len(sm.groupToShardsSlice)>NShards {
					newGroupsCount = NShards - len(sm.groupToShardsSlice)
				}			
				//how many shards will be moved
				movedShardsCount := aveShardsNum*newGroupsCount	
				movedShards := make([]int,0)
				//moved shards from old groups to new groups, now collect
				for k,_:=range sm.groupToShardsSlice{
					if(sm.groupToShardsSlice[k].shardsNum>aveShardsNum){
						min := sm.groupToShardsSlice[k].shardsNum-aveShardsNum
						if min > movedShardsCount{
							min = movedShardsCount
						}
						movedShards = append(movedShards,sm.groupToShardsSlice[k].shards[0:min]...)
						sm.groupToShardsSlice[k].shards = sm.groupToShardsSlice[k].shards[min:]
						sm.groupToShardsSlice[k].shardsNum -= min
					
						movedShardsCount -= min
						if movedShardsCount==0 {
							goto EndMoved
						}
					}
				}
			EndMoved:
				//moved shards from old groups to new groups, now move
				k:=0
				for i:=0;i<newGroupsCount;i++{
					for j:=0;j<aveShardsNum;j++{
						newGroupToShardsSlice[i].shards = append(newGroupToShardsSlice[i].shards,movedShards[k])
						newGroupToShardsSlice[i].shardsNum += 1
						config.Shards[movedShards[k]] = newGroupToShardsSlice[i].groupId
						k++
					}
				}				
			}
		}
		//append new GroupToShardsSlice 
		sm.groupToShardsSlice = append(sm.groupToShardsSlice,newGroupToShardsSlice...)	
		//append new config 
		sm.configs = append(sm.configs,config)
	case LEAVE:
		// new config
		var config Config
		config.Num = sm.getLastConfigNum()+1
		copy(config.Shards[0:],sm.configs[config.Num-1].Shards[0:])
		config.Groups = make(map[int][]string)
		//copy from old config: create a new map object (with make()) and copy the keys and values individually
		oldGroups := sm.configs[config.Num-1].Groups
		for gid,_:=range oldGroups{
			//not include those groups
			for i:=0;i<len(args.GIDs);i++ {
				if gid==args.GIDs[i]{
					goto Next_Group
				}
			}
			config.Groups[gid] = make([]string,len(oldGroups[gid]))
			copy(config.Groups[gid],oldGroups[gid])
		Next_Group:
		}
		//fmt.Println("in leave: ",args.ClerkId,args.ReqId,config.Groups,oldGroups)
		//cut the moved groups from groupToShardsSlice
		movedShards := make([]int,0)
		for i:=0;i<len(args.GIDs);i++ {
			for j:=0;j<len(sm.groupToShardsSlice);j++ {
				if args.GIDs[i] == sm.groupToShardsSlice[j].groupId {
					movedShards = append(movedShards,sm.groupToShardsSlice[j].shards...)
					sm.groupToShardsSlice=append(sm.groupToShardsSlice[:j],sm.groupToShardsSlice[j+1:]...)
					goto Next_GID
				}
			}
		Next_GID:
		}

		//sort groups by its shardsNum desc
		sort.Sort(sm.groupToShardsSlice)
		//moved to other groups and change config.Shards
		//moved to the group first which has less shards
		k:=0
		for;k<len(movedShards);{
			//moved to the group first which has less shards
			for i:=len(sm.groupToShardsSlice)-1;i>=0;i--{
				if k<len(movedShards){
					sm.groupToShardsSlice[i].shards = append(sm.groupToShardsSlice[i].shards,movedShards[k])
					sm.groupToShardsSlice[i].shardsNum += 1
					config.Shards[movedShards[k]] = sm.groupToShardsSlice[i].groupId
					k++
				}else{
					break
				}
			}
		}
		//append new config 
		sm.configs = append(sm.configs,config)
	case MOVE:
		// new config
		var config Config
		config.Num = sm.getLastConfigNum()+1
		copy(config.Shards[0:],sm.configs[config.Num-1].Shards[0:])
		config.Groups = make(map[int][]string)
		//copy from old config: create a new map object (with make()) and copy the keys and values individually
		oldGroups := sm.configs[config.Num-1].Groups
		for gid,_:=range oldGroups{ 
			config.Groups[gid] = make([]string,len(oldGroups[gid]))
			copy(config.Groups[gid],oldGroups[gid])
		}
		//if need to move
		if args.Shard>=0 && args.Shard<NShards && config.Shards[args.Shard]!=args.GID {
			source:=-1
			target:=-1
			sourceShardIndex:=-1
			for i:=len(sm.groupToShardsSlice)-1;i>=0;i--{
				if sm.groupToShardsSlice[i].groupId == config.Shards[args.Shard]{
					source = i
					for k,_:=range sm.groupToShardsSlice[i].shards{
						if sm.groupToShardsSlice[i].shards[k]==args.Shard{
							sourceShardIndex = k
						}
					}
				}
				if sm.groupToShardsSlice[i].groupId == args.GID{
					target = i
				}
			}
			//now move
			if source!=-1 && target!=-1 {
				sm.groupToShardsSlice[source].shards = append(sm.groupToShardsSlice[source].shards[:sourceShardIndex],sm.groupToShardsSlice[source].shards[sourceShardIndex+1:]...)
				sm.groupToShardsSlice[source].shardsNum -= 1
				sm.groupToShardsSlice[target].shards = append(sm.groupToShardsSlice[target].shards,args.Shard)
				sm.groupToShardsSlice[target].shardsNum += 1
				config.Shards[args.Shard] = args.GID
			}
		}
		//fmt.Println("in move: ",args.ClerkId,args.ReqId,config.Groups,oldGroups)
		//append new config 
		sm.configs = append(sm.configs,config)
	case QUERY:
	}	
	//maxAck,ok := sm.ackMap[args.ClerkId]
	//fmt.Println("in storePair, me, sm.ackMap, ok, old maxAck, reqId, accept op =",sm.me,sm.ackMap,ok,maxAck,args.ReqId,args)
	//store the max reqId
	sm.ackMap[args.ClerkId] = args.ReqId 
}

func checkNewApply(newApply Op, oldArgs Op) bool{
	//return newApply == oldArgs
	//only need clerkId and reqId to check!!
	return newApply.Op == oldArgs.Op &&
		newApply.ClerkId == oldArgs.ClerkId &&
		newApply.ReqId == oldArgs.ReqId 
}

func (sm *ShardMaster) callRaft(args Op)(wrongLeader bool) {
	//fmt.Println("in callRaft,me,command=",sm.me,args) 
	index,_,isLeader := sm.rf.Start(args)
	if !isLeader{ 
		return true
	}
	
	//fmt.Println("in callRaft,isLeader,me,index,command=",isLeader,sm.me,index,args) 
	sm.mu.Lock()  
	if _,ok := sm.rpcResult[index];!ok{  
		sm.rpcResult[index] = make(chan Op,1)
	}
	ch := sm.rpcResult[index] 
	sm.mu.Unlock() //unlock before reading from channel, or may cause deadlock

	// wait for notification
	// raft may change leader while smServer is waiting for notification, even though server is waiting for the index to be committed, not the leader,
	// it may cause some problem if smServer simply blocks to wait without timeout.
	// i.e. raft leader accepts some command, and crashes, then a new leader is elected. smServer simply blocks to wait without timeout, no longer send new command.
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
	// args may be different from sm.rpcResult[index].
	// i.e. At the beginning, smServer A is the leader, it accept a request(log index is index1), and waits for raft's commit. 
	// Unfortunately, network partition happens.
	// smServer A is now in the minority partition, a new raft leader in the majority partition is elected,
	// but smServer A still blocks in select-case and still within the timeout period.
	// In the meantime, the new leader accepts new commands and commit. And then, the partition heals.
	// Now smServer A may accept the leader's log entry at index1, which is different from the old request that smServer A is waiting for .

	for{
		select{
		case op := <- ch:
			//fmt.Println("in callRaft,index,new log entry,old request=",index,op,args) 
			//fmt.Println("in callRaft,me,index,if same,args =",sm.me,index,op==args,args) 
			return !checkNewApply(op,args)
		case <- time.After(20 * time.Millisecond):
			_, isLeader := sm.rf.GetState()
			if !isLeader{
				return true
			}
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var op Op
	op.Op = JOIN 
	op.ClerkId = args.ClerkId
	op.ReqId = args.ReqId
	op.Servers = args.Servers

	//append to log
	wrongLeader := sm.callRaft(op) 
	if wrongLeader{
		reply.WrongLeader = true
		return
	}
 	//reply to clerk
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var op Op
	op.Op = LEAVE 
	op.ClerkId = args.ClerkId
	op.ReqId = args.ReqId
	op.GIDs = args.GIDs

	//append to log
	wrongLeader := sm.callRaft(op) 
	if wrongLeader{
		reply.WrongLeader = true
		return
	}
 	//reply to clerk
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var op Op
	op.Op = MOVE 
	op.ClerkId = args.ClerkId
	op.ReqId = args.ReqId
	op.Shard = args.Shard
	op.GID = args.GID

	//append to log
	wrongLeader := sm.callRaft(op) 
	if wrongLeader{
		reply.WrongLeader = true
		return
	}
 	//reply to clerk
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var op Op
	op.Op = QUERY 
	op.ClerkId = args.ClerkId
	op.ReqId = args.ReqId
	op.Num = args.Num

	//append to log
	wrongLeader := sm.callRaft(op) 
	if wrongLeader{
		reply.WrongLeader = true
		return
	}
 	//reply to clerk
	reply.WrongLeader = false
	reply.Err = OK
	sm.mu.Lock() 
	if args.Num < 0 {
		reply.Config = sm.configs[sm.getLastConfigNum()]		
	} else{
		reply.Config = sm.configs[args.Num]	
	}
	sm.mu.Unlock()
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsm tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.configs[0].Num = 0
	sm.groupToShardsSlice = make(GroupToShardsSlice,0)
	sm.rpcResult = make(map[int]chan Op)
	sm.ackMap = make(map[int64]int64)
	//fmt.Println("StartsmServer")
	go func() {
		for m := range sm.applyCh {  
			//do not call raft's function who holds raft's lock here,like sm.rf.GetState(), may cause deadlock.
			//i.e. raft holds its lock, and tries to write into ApplyMsg channel whenever it has new commit,
			//if we call sm.rf.GetState() before reading from ApplyMsg channel,
			//sm.rf.GetState() will block in tring to acquire raft's lock, but raft will not unlock until we read from ApplyMsg channel
			
			if m.UseSnapshot {
			} else {
				op := m.Command.(Op)
				sm.mu.Lock() 
				// checkRepeatedRequest() and store() must be execed within the same lock.
				// Otherwise, server may store the same repeated request twice: after the firt checkRepeatedRequest(), 
				// and before the first store(), call checkRepeatedRequest() again, the result is without repetition.
				// deal with repeated request
				if isRepeated := sm.checkRepeatedRequest(op);!isRepeated{ 
					// store the key-value pair
					sm.store(op)
				}	

				// to notify
				if _,ok:=sm.rpcResult[m.Index];!ok{
					sm.rpcResult[m.Index] = make(chan Op,1)
				} 
				ch := sm.rpcResult[m.Index]
				sm.mu.Unlock()// unlock before write into channel, or may cause deadlock

				ch <- op // write into every server, but only read from leader
				//fmt.Println("in applyCh,index,me,clerkId,reqId,op=",m.Index,sm.me,op.ClerkId,op.ReqId,op)
			}   
		}
	}()
	return sm
}
