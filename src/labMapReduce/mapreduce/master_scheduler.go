package mapreduce

import (
	"log"
	"sync"
)

// Schedules map operations on remote workers. This will run until InputFilePathChan
// is closed. If there is no worker available, it'll block.
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) int {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////

	var (
		wg              sync.WaitGroup
		filePath        string
		worker          *RemoteWorker
		operation       *Operation
		failedOperation *Operation
		counter         int
	)

	log.Printf("Scheduling %v operations\n", proc)

	for filePath = range filePathChan {
		operation = &Operation{proc, counter, filePath}
		counter++

		worker = <-master.idleWorkerChan
		wg.Add(1)
		go master.runOperation(worker, operation, &wg, false)
	}

	wg.Wait()

	if master.failedOpCounter > 0 {
		for failedOperation = range master.failedOperationsChan {
			worker = <-master.idleWorkerChan
			wg.Add(1)
			go master.runOperation(worker, failedOperation, &wg, true)
		}
		wg.Wait()
	}

	log.Printf("%vx %v operations completed\n", counter, proc)
	return counter
}

// runOperation start a single operation on a RemoteWorker and wait for it to return or fail.
func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation, wg *sync.WaitGroup, failedBefore bool) {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////

	var (
		err  error
		args *RunArgs
	)

	if failedBefore {
		log.Printf("Running again %v (ID: '%v' File: '%v' Worker: '%v')\n", operation.proc, operation.id, operation.filePath, remoteWorker.id)
	} else {
		log.Printf("Running %v (ID: '%v' File: '%v' Worker: '%v')\n", operation.proc, operation.id, operation.filePath, remoteWorker.id)

	}

	args = &RunArgs{operation.id, operation.filePath}
	err = remoteWorker.callRemoteWorker(operation.proc, args, new(struct{}))

	if err != nil {
		log.Printf("Operation %v '%v' Failed. Error: %v\n", operation.proc, operation.id, err)
		master.failedOperationsChan <- operation
		master.failedWorkerChan <- remoteWorker
		if !failedBefore {
			master.failedCounterMutex.Lock()
			master.failedOpCounter++
			master.failedCounterMutex.Unlock()
		}
		wg.Done()
	} else {
		master.idleWorkerChan <- remoteWorker
		if failedBefore {
			master.failedCounterMutex.Lock()
			master.failedOpCounter--
			if master.failedOpCounter == 0 {
				close(master.failedOperationsChan)
			}
			master.failedCounterMutex.Unlock()
		}
		master.successCounterMutex.Lock()
		master.successfulOpCounter++
		master.successCounterMutex.Unlock()
		wg.Done()
	}
}
