// Copyright 2020 ConsenSys Software Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parallel

import (
	"runtime"
	"sync"
)

var executor *ParallelExecutor

type WorkFunc func(int, int)

type parallelExecutorInput struct {
	start, end int
	work WorkFunc
}

type parallelExecutor struct {
	workChs []chan<-parallelExecutorInput
	closeCh []chan<-struct{}
}

func workerLoop(workCh <-chan parallelExecutorInput, closeCh <-chan struct{}) {
	for {
	case newWork := <-workCh:
		newWork.work(newWork.start, newWork.end)
	case <-exitCh:
		return
	}
}

func NewParallelExecutor(numGoroutines int) *ParallelExecutor {
	workChs := []chan<-parallelExecutorInput{}
	closeChs := []chan<-struct{}{}

	for i := 0; i < numGoroutines; i++ {
		workCh := make(chan<-parallelExecutorInput)
		closeCh := make(chan<-struct{})

		workChs := append(workChs, workCh)
		workChs := append(workChs, closeCh)
		go workerLoop(workCh, closeCh)
	}

	return &ParallelExecutor{
		workChs,
		closeChs,
	}
}

func (p* ParallelExecutor) Close() {
	for i := 0; i < len(p.workerCloseChs); i++ {
		close(p.workerCloseChs[i])
	}
}

func (p *ParallelExecutor) Execute(nbIterations int, work func(int, int)) int {
	nbTasks := len(p.workChs)
	nbIterationsPerCpus := nbIterations / nbTasks

	// more CPUs than tasks: a CPU will work on exactly one iteration
	if nbIterationsPerCpus < 1 {
		nbIterationsPerCpus = 1
		nbTasks = nbIterations
	}

	extraTasks := nbIterations - (nbTasks * nbIterationsPerCpus)
	extraTasksOffset := 0

	for i := 0; i < nbTasks; i++ {
		_start := i*nbIterationsPerCpus + extraTasksOffset
		_end := _start + nbIterationsPerCpus
		if extraTasks > 0 {
			_end++
			extraTasks--
			extraTasksOffset++
		}
		p.workChs[i] <- parallelExecutorInput{_start, _end, work}
	}
	return nbTasks
}

func Init(numGoroutines int) {
	executor = NewParallelExecutor(numGoroutines)
}

func Close() {
	executor.Close()
	executor = nil
}

func Execute(nbIterations int, work func(int, int)) int {
	return executor.Execute(nbIterations, work)
}
