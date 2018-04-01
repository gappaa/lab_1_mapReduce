package mapreduce

import (
	"fmt"
	logger "github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
)

type Task struct {
	file string
	number int
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
	logger.WithFields(logger.Fields{
		"nTasks": ntasks,
		"nOther": n_other,
	}).Info("[schedule] Init schedule")

	workers := make(chan string, 10)
	tasks := make(chan Task, ntasks)
	stopWorker := make(chan struct{})
	atomicIsClose := int32(0)

	remainingTasks := &sync.WaitGroup{}
	remainingTasks.Add(ntasks)
	
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// Add tasks
	go func(tasksChan chan Task, mapFiles []string, nbTasks int) {
		for cpt, file := range mapFiles {
			if cpt < nbTasks {
				logger.WithField("file", file).Info("[schedule] add task")
				if atomic.LoadInt32(&atomicIsClose) == 0 {
					tasks <- Task{file: file, number: cpt}
				}
			}
		}
	}(tasks, mapFiles, ntasks)

	// Add workers
	go func(workers chan string, register chan string, stopWorker chan struct{}) {
		for {
			select {
			case element, ok := <-register:
				if ok {
					logger.WithField("worker", element).Info("[schedule] add worker")
					if atomic.LoadInt32(&atomicIsClose) == 0 {
						workers <- element
					}
				} else {
					break
				}
			case <- stopWorker:
				return
			}
		}
	}(workers, registerChan, stopWorker)

	// Give task
	go func(workers chan string, tasks chan Task, waitGroup *sync.WaitGroup) {
		for {
			select {
			case worker, ok := <- workers:
				if !ok {
					return
				}
				go func(worker string, tasks chan Task, workers chan string, waitGroup *sync.WaitGroup) {
					select {
					case taskFile, ok := <- tasks:
						if !ok {
							return
						}
						task := DoTaskArgs{
							JobName: jobName,
							Phase: phase,
							TaskNumber: taskFile.number,
							NumOtherPhase: n_other,
						}

						if phase == mapPhase {
							task.File = taskFile.file
						}

						logger.WithFields(logger.Fields{
							"worker": worker,
							"task": task,
						}).Info("[schedule] file send")

						if call(worker, "Worker.DoTask", task, nil) {
							logger.WithFields(logger.Fields{
								"worker": worker,
								"task": task,
							}).Info("[schedule] task success")
							if atomic.LoadInt32(&atomicIsClose) == 0 {
								workers <- worker
							}
							waitGroup.Done()
						} else {
							logger.WithFields(logger.Fields{
								"worker": worker,
								"task": task,
							}).Info("[schedule] task error")
							if atomic.LoadInt32(&atomicIsClose) == 0 {
								workers <- worker
							}
							if atomic.LoadInt32(&atomicIsClose) == 0 {
								tasks <- taskFile
							}
						}
					}

				}(worker, tasks, workers, waitGroup)
			}
		}
	}(workers, tasks, remainingTasks)

	// Task all finished
	remainingTasks.Wait()
	logger.Info("[schedule] END")
	atomic.AddInt32(&atomicIsClose, 1)
	stopWorker <- struct{}{}
	close(workers)
	close(tasks)
	close(stopWorker)


	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
