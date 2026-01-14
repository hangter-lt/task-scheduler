package scheduler

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/hangter-lt/task-scheduler/executor"
	"github.com/hangter-lt/task-scheduler/task"
	"github.com/hangter-lt/task-scheduler/types"
)

// Scheduler 任务调度器，负责管理和调度各种类型的任务
type Scheduler struct {
	heap           *TaskHeap            // 任务最小堆，按下次执行时间排序
	taskMap        map[string]task.Task // 任务ID->任务的映射，用于快速查找和取消任务
	cancelledTasks map[string]task.Task // 已取消任务的映射，用于恢复任务
	mu             sync.Mutex           // 并发锁，保护共享资源
	executor       *executor.Executor   // 任务执行器，用于实际执行任务
	stopCh         chan struct{}        // 停止信号通道，用于优雅关闭调度器
	isRunning      bool                 // 运行状态标志
}

// NewScheduler 创建一个新的任务调度器
// exec: 关联的任务执行器
func NewScheduler(exec *executor.Executor) *Scheduler {
	h := &TaskHeap{}
	heap.Init(h)

	return &Scheduler{
		heap:           h,
		taskMap:        make(map[string]task.Task),
		cancelledTasks: make(map[string]task.Task),
		mu:             sync.Mutex{},
		executor:       exec,
		stopCh:         make(chan struct{}),
		isRunning:      false,
	}
}

// Register 注册任务到调度器
// t: 要注册的任务，必须实现Task接口
func (s *Scheduler) Register(t task.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查任务是否已存在
	if _, ok := s.taskMap[t.ID()]; ok {
		return
	}

	// 添加任务到最小堆
	heap.Push(s.heap, t)
	// 添加任务到映射
	s.taskMap[t.ID()] = t
}

// Cancel 取消指定ID的任务
// id: 要取消的任务ID
func (s *Scheduler) Cancel(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 查找任务
	t, ok := s.taskMap[id]
	if !ok {
		return
	}

	// 从映射中移除任务
	delete(s.taskMap, id)

	// 从最小堆中移除任务
	for i := 0; i < s.heap.Len(); i++ {
		if (*s.heap)[i].ID() == id {
			heap.Remove(s.heap, i)
			break
		}
	}

	// 将任务保存到已取消任务映射中
	s.cancelledTasks[id] = t
}

// Resume 恢复已取消的任务
// id: 要恢复的任务ID
func (s *Scheduler) Resume(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 从已取消任务映射中查找任务
	t, ok := s.cancelledTasks[id]
	if !ok {
		return
	}

	// 从已取消任务映射中移除
	delete(s.cancelledTasks, id)

	// 确保下次执行时间正确
	// 对于一次性任务，如果执行时间已过，使用当前时间
	// 对于周期任务，重新计算下次执行时间
	if t.NextExecTime().Before(time.Now()) {
		if t.Type() == types.TaskTypeOnce {
			t.SetNextExecTime(time.Now())
		} else {
			t.UpdateNextExecTime()
		}
	}

	// 重新添加到任务队列
	s.taskMap[id] = t
	heap.Push(s.heap, t)
}

// Run 启动调度器
// 启动后会持续运行，直到调用Stop方法
func (s *Scheduler) Run() {
	s.mu.Lock()

	if s.isRunning {
		s.mu.Unlock()
		return
	}
	s.isRunning = true
	s.mu.Unlock()

	for {
		select {
		case <-s.stopCh:
			// 接收到停止信号，关闭调度器
			s.mu.Lock()
			s.isRunning = false
			s.mu.Unlock()
			return
		default:
			s.mu.Lock()
			// 无任务时休眠
			if s.heap.Len() == 0 {
				s.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// 获取堆顶任务（下次执行时间最早的任务）
			nextTask := s.heap.Peek()
			now := time.Now()
			waitDur := nextTask.NextExecTime().Sub(now)
			s.mu.Unlock()

			// 等待任务执行时间
			if waitDur > 0 {
				select {
				case <-time.After(waitDur):
					// 时间到，继续执行
				case <-s.stopCh:
					// 等待期间接收到停止信号，退出
					return
				}
				continue
			}

			// 执行任务
			s.mu.Lock()
			execTask := heap.Pop(s.heap).(task.Task)
			delete(s.taskMap, execTask.ID())
			s.mu.Unlock()

			// 提交任务到执行器异步执行
			go s.executeTask(execTask)
		}
	}
}

// executeTask 执行任务
// t: 要执行的任务
func (s *Scheduler) executeTask(t task.Task) {
	// 创建带超时的上下文
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if t.Timeout() > 0 {
		// 任务有超时设置
		ctx, cancel = context.WithTimeout(context.Background(), t.Timeout())
	} else {
		// 任务无超时设置
		ctx, cancel = context.WithCancel(context.Background())
	}

	// 提交到异步执行器
	err := s.executor.Submit(ctx, t, func(execErr error) {
		// 任务完成后取消上下文
		defer cancel()
		// 处理任务执行结果
		s.handleTaskResult(t, execErr)
	})

	if err != nil {
		// 提交任务失败
		cancel()
		s.handleTaskResult(t, err)
	}
}

// handleTaskResult 处理任务执行结果
// t: 执行的任务
// execErr: 执行错误，nil表示成功
func (s *Scheduler) handleTaskResult(t task.Task, execErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 失败重试逻辑
	if execErr != nil && t.RetryPolicy().MaxRetry > t.RetryPolicy().CurrentRetry {
		// 增加重试次数
		t.RetryPolicy().CurrentRetry++
		// 计算重试执行时间（当前时间+重试间隔）
		retryTime := time.Now().Add(t.RetryPolicy().RetryDelay)
		t.SetNextExecTime(retryTime)
		// 重新添加到任务队列
		s.taskMap[t.ID()] = t
		heap.Push(s.heap, t)
		return
	}

	// 成功重置重试次数
	t.ResetRetry()

	// 周期任务, 计算下次执行时间
	if t.Type() == types.TaskTypeCron {
		t.UpdateNextExecTime()
		// 重新添加到任务队列
		s.taskMap[t.ID()] = t
		heap.Push(s.heap, t)
		return
	}
}

// Stop 停止调度器
// 会发送停止信号，调度器会在处理完当前任务后退出
func (s *Scheduler) Stop() {
	close(s.stopCh)
}

// IsRunning 检查调度器是否正在运行
func (s *Scheduler) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isRunning
}
