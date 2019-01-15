package mapreduce

import (
	"fmt"
)

type Result struct {
	worker string
	status bool
	work int
}

func findUnfinishedWork(workStatus *[]int) int {
	for index, val := range (*workStatus) {
		if val == 0 {
			return index
		}
	}
	return -1
}

func dispatchTask(jobName string, mapFiles []string, currentTask int, address string, n_other int, phase jobPhase, success chan Result, fail chan Result) {
	taskArgs := DoTaskArgs{jobName, "", phase, 0, n_other}
	taskResult := Result{address, true, currentTask}

	taskArgs.TaskNumber = currentTask		
	if phase == mapPhase {
		taskArgs.File = mapFiles[taskArgs.TaskNumber]
	}
	
	status := call(address, "Worker.DoTask", taskArgs, nil)
	
	if status {		
		taskResult.status = true
		success <- taskResult
	} else {		
		taskResult.status = false
		fail <- taskResult
	}
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
	
	workers := make([]string, 0, 10)
	workStatus := make([]int, ntasks)
	finishedTasks := 0
	
	success := make(chan Result)
        fail := make(chan Result)
	
	defer close(success)
	defer close(fail)
	
	handleLoop:
	for {
		select {
			case registerStr := <- registerChan:
				fmt.Printf("Register worker %s\n", registerStr)
				work := findUnfinishedWork(&workStatus)
				if work == -1 {
					workers = append(workers, registerStr)
				} else {
					workStatus[work] = 1
					go dispatchTask(jobName, mapFiles, work, registerStr, n_other, phase, success, fail)	
				}					
			case taskReturn := <- success:
				workStatus[taskReturn.work] = 2
				
				finishedTasks++
				
				if finishedTasks == ntasks {
					fmt.Println("All tasks finished!")	
					break handleLoop
				} else
				{
					work := findUnfinishedWork(&workStatus)
					if work == -1 {
						workers = append(workers, taskReturn.worker)
					} else {
						workStatus[work] = 1
						go dispatchTask(jobName, mapFiles, work, taskReturn.worker, n_other, phase, success, fail)	
					}
				}
			case taskReturn := <- fail:
				workStatus[taskReturn.work] = 0

				if len(workers) == 0 {
					fmt.Println("No available workers!")
				} else	{
					workStatus[taskReturn.work] = 1
					worker := workers[len(workers) - 1]
					workers = workers[:len(workers) - 1]
					go dispatchTask(jobName, mapFiles, taskReturn.work, worker, n_other, phase, success, fail)
				}
		}
	}
}
