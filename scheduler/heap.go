package scheduler

import "github.com/hangter-lt/task-scheduler/task"

// 最小堆
type TaskHeap []task.Task

// 实现container/heap接口
func (h TaskHeap) Len() int           { return len(h) }
func (h TaskHeap) Less(i, j int) bool { return h[i].NextExecTime() < h[j].NextExecTime() }
func (h TaskHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *TaskHeap) Push(x any) {
	*h = append(*h, x.(task.Task))
}

func (h *TaskHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *TaskHeap) Peek() task.Task {
	if len(*h) == 0 {
		return nil
	}
	return (*h)[0]
}
