package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

// track whether workers executed in parallel.
type TaskStatus struct {
	mu  sync.Mutex
	status []int	// 0: not issued 1: being handled 2: succeded
	phase jobPhase
	finishedTask int
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	
	var taskStatus TaskStatus
	taskStatus.status = make([]int, ntasks)
	taskStatus.phase = phase
	
	quit := make(chan int)
	
	var waitGroup sync.WaitGroup 
	
	go func() {
		for {
			select {
			case registerStr := <- registerChan:
				go dispatchTask(jobName, mapFiles, &taskStatus, registerStr, n_other, quit, &waitGroup)
			case <- quit:
				return
			}
		}
	}()
	
	for {
		//Sleep to before waiting
		time.Sleep(100 * time.Millisecond)
		waitGroup.Wait()
		
		if taskStatus.finishedTask == ntasks {
			quit <- 1
			fmt.Println("All task finished. Stop listening")
			break
		} else {
			fmt.Println("Task unfinihsed because lost of connection. Waiting for connecting again")
		}
	}

	
	time.Sleep(100 * time.Millisecond)
}

func dispatchTask(jobName string, mapFiles []string, taskStatus *TaskStatus, address string, n_other int, quit chan int, waitGroup *sync.WaitGroup) {
	(*waitGroup).Add(1)
	defer (*waitGroup).Done()
	
	taskArgs := DoTaskArgs{jobName, "", (*taskStatus).phase, 0, n_other}
	
	goodConnection := true
	hasUnfinishedTask := true

	for goodConnection && hasUnfinishedTask {	
		currentTask := -1

		//Find the next unfinishsed task
		(*taskStatus).mu.Lock()
		hasUnfinishedTask = false
		for index, st := range (*taskStatus).status {
			if st == 1 {										
				hasUnfinishedTask = true
			} else if st == 0 {
				hasUnfinishedTask = true
				currentTask = index
				break
			}
		} 
		
		// Set the current task to in progress
		if currentTask != -1 {
			(*taskStatus).status[currentTask] = 1
		}

		(*taskStatus).mu.Unlock()
		
		// If no availble task but some are still unfinihsed, we sleep and wait for the next round
		if currentTask == -1 && hasUnfinishedTask {
			time.Sleep(100 * time.Millisecond)			
			continue
		} else if hasUnfinishedTask {
			taskArgs.TaskNumber = currentTask		
			if (*taskStatus).phase == mapPhase {
				taskArgs.File = mapFiles[taskArgs.TaskNumber]
			}
				
			goodConnection = call(address, "Worker.DoTask", taskArgs, nil)
			
			(*taskStatus).mu.Lock()

			if goodConnection {
				(*taskStatus).status[currentTask] = 2
				(*taskStatus).finishedTask++
			} else {		
				(*taskStatus).status[currentTask] = 0
			}
			
			(*taskStatus).mu.Unlock()
		}
	}

	if !goodConnection {
		fmt.Printf("%s: Return because connection stopped!\n", address)
	} else {
		fmt.Printf("%s: Return because all tasks finish\n", address)
	}
}


