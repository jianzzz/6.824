package zj

import (
	"testing" 
	"fmt"
	"sync"
	"time"
)

type ApplyMsg struct {
	Index       int 
}

type RaftKV struct {  
	mu      sync.Mutex
	applyCh chan ApplyMsg
	rpcResult map[int]chan bool 
	index int
}

func (kv *RaftKV) generate() int{
	kv.mu.Lock() 
	defer kv.mu.Unlock()

	kv.index++
	go func(){
		var apply ApplyMsg
		apply.Index = kv.index  
		kv.applyCh <- apply 
	}() 
	return kv.index
}

func (kv *RaftKV) func1() { 
	go func() {
		for m := range kv.applyCh {    
			kv.mu.Lock() 
			// to notify
			if _,ok:=kv.rpcResult[m.Index];!ok{
fmt.Println("in func1,make index=",m.Index) 
				kv.rpcResult[m.Index] = make(chan bool,1)
			}
			ch := kv.rpcResult[m.Index]
			kv.mu.Unlock()// unlock before write into channel, or may cause deadlock
  
			ch <- true // write into every server, but only read from leader 
			fmt.Println("in func1,index=",m.Index) 
		}
	}() 
}

func (kv *RaftKV) func2() { 
	index := kv.generate() 
//time.Sleep(50 )
	kv.mu.Lock()  
	if _,ok := kv.rpcResult[index];!ok{  
fmt.Println("in func2,make index=",index) 
		//kv.rpcResult[index] = make(chan bool,1)
	}
	ch := kv.rpcResult[index] 
	kv.mu.Unlock() //unlock before reading from channel, or may cause deadlock

	// wait for notification 
	<- ch
	fmt.Println("in func2,index=",index) 
}

func TestBasic(t *testing.T) { 
	var kv RaftKV
	kv.applyCh = make(chan ApplyMsg) 
	kv.rpcResult = make(map[int]chan bool) 
	kv.index = 0

	kv.func1() 
	go func(){
		for {
			for i:=0;i<5;i++{
				//select{
				//case <- time.After(5 * time.Millisecond):  
					kv.func2()
				//}
			} 
		}
	}()
	time.Sleep(100 * time.Second)
}



func TestChannel(t *testing.T) { 
	var ch map[int]chan int;
	ch = make(map[int]chan int)
	for i:=0;i<5;i++{
		if _,ok:=ch[i];!ok{
			ch[i] = make(chan int,2)
		} 
		ch[i] <- i
		fmt.Println(1)
		ch[i] <- i
		fmt.Println(2)
	}
}





