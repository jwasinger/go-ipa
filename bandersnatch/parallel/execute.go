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

type WorkFunc func(int, int)

type parallelExecuterInput struct {
	start, end int
	work WorkFunc
}

type ParallelExecuter struct {
	workChs []chan parallelExecuterInput
	closeChs []chan struct{}
}

var executer *ParallelExecuter

func workerLoop(workCh <-chan parallelExecuterInput, exitCh <-chan struct{}) {
	for {
		select {
		case newWork := <-workCh:
			newWork.work(newWork.start, newWork.end)
		case <-exitCh:
			return
		}
	}
}

func NewParallelExecuter(numGoroutines int) *ParallelExecuter {
	workChs := []chan parallelExecuterInput{}
	closeChs := []chan struct{}{}

	for i := 0; i < numGoroutines; i++ {
		workCh := make(chan parallelExecuterInput)
		closeCh := make(chan struct{})

		workChs = append(workChs, workCh)
		closeChs = append(closeChs, closeCh)
		go workerLoop(workCh, closeCh)
	}

	return &ParallelExecuter{
		workChs,
		closeChs,
	}
}

func (p* ParallelExecuter) Close() {
	for i := 0; i < len(p.closeChs); i++ {
		close(p.closeChs[i])
	}
}

func (p *ParallelExecuter) Execute(nbIterations int, work func(int, int)) int {
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
		p.workChs[i] <- parallelExecuterInput{_start, _end, work}
	}
	return nbTasks
}

func Init(numGoroutines int) {
	executer = NewParallelExecuter(numGoroutines)
}

func Close() {
	executer.Close()
	executer = nil
}

func Execute(nbIterations int, work func(int, int)) int {
	return executer.Execute(nbIterations, work)
}
