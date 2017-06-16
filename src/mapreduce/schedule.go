package mapreduce

import(
	"fmt"
	"sync"
) 

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	var wg sync.WaitGroup
	for task:=0;task<ntasks;task++{
		wg.Add(1)
		
		go func(taskNum int){
			for{//we use this, cause worker may fail
				//wait until we have available worker
				worker := <- registerChan 		

				var arg DoTaskArgs
				arg.JobName = jobName
				arg.Phase = phase
				arg.File = mapFiles[taskNum]
				arg.TaskNumber = taskNum
				arg.NumOtherPhase = n_other
				ok := call(worker, "Worker.DoTask", &arg, nil)
				if ok == false {
					fmt.Printf("Cleanup: RPC %s error\n", worker)
				}else{
					//remember to use goruntine to write data into registerChan
					//because last time we write data into it, it will block until someone read it
					//however, no one read from it, so the last wg.Done() will not been executed
					//and map-phase never finish
					go func(){
						//again, worker is free
						registerChan <- worker
					}()
					wg.Done()
					break
				}
			} 
		}(task)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
