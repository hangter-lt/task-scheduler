package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hangter-lt/task-scheduler/executor"
	"github.com/hangter-lt/task-scheduler/persistence"
	"github.com/hangter-lt/task-scheduler/task"
)

// Scheduler 任务调度器，负责管理和调度各种类型的任务
type Scheduler struct {
	heap           *TaskHeap                       // 任务最小堆，按下次执行时间排序
	taskMap        map[string]task.Task            // 任务ID->任务的映射，用于快速查找和取消任务
	cancelledTasks map[string]task.Task            // 已取消任务的映射，用于恢复任务
	failureRecords map[string][]task.FailureRecord // 任务失败记录，key为任务ID，value为失败记录列表
	mu             sync.Mutex                      // 并发锁，保护共享资源
	executor       *executor.Executor              // 任务执行器，用于实际执行任务
	stopCh         chan struct{}                   // 停止信号通道，用于优雅关闭调度器
	isRunning      bool                            // 运行状态标志
	persistence    *persistence.RedisPersistence   // Redis持久化层
	nodeFlag       string                          // 节点标识
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
		failureRecords: make(map[string][]task.FailureRecord),
		mu:             sync.Mutex{},
		executor:       exec,
		stopCh:         make(chan struct{}),
		isRunning:      false,
	}
}

// NewSchedulerWithPersistence 创建一个带有Redis持久化的任务调度器
// exec: 关联的任务执行器
// persistence: Redis持久化层
func NewSchedulerWithPersistence(exec *executor.Executor, persistence *persistence.RedisPersistence, nodeFlag string) *Scheduler {
	h := &TaskHeap{}
	heap.Init(h)

	s := &Scheduler{
		heap:           h,
		taskMap:        make(map[string]task.Task),
		cancelledTasks: make(map[string]task.Task),
		failureRecords: make(map[string][]task.FailureRecord),
		mu:             sync.Mutex{},
		executor:       exec,
		stopCh:         make(chan struct{}),
		isRunning:      false,
		nodeFlag:       nodeFlag + "-" + uuid.New().String(),
		persistence:    persistence,
	}

	// 从Redis加载所有任务
	if persistence != nil {
		tasks, err := persistence.LoadAllTasks()
		if err == nil {
			for _, t := range tasks {
				if t.Status() == task.TaskStatusPending || t.Status() == task.TaskStatusRunning {
					s.taskMap[t.ID()] = t
					heap.Push(s.heap, t)
				}
			}
		}
	}

	return s
}

// Register 注册任务到调度器
// t: 要注册的任务，必须实现Task接口
func (s *Scheduler) Register(t task.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查任务是否已存在
	if _, ok := s.taskMap[t.ID()]; ok {
		// 任务已存在，更新任务以及redis中的任务
		s.taskMap[t.ID()] = t
		if s.persistence != nil {
			s.persistence.SaveTask(t)
		}
		return
	}

	// 添加任务到最小堆
	heap.Push(s.heap, t)
	// 添加任务到映射
	s.taskMap[t.ID()] = t

	// 保存到Redis持久化
	if s.persistence != nil {
		s.persistence.SaveTask(t)
	}
}

// Suspend 挂起指定ID的任务
// id: 要挂起的任务ID
func (s *Scheduler) Suspend(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 查找任务
	fmt.Printf("s.taskMap: %v\n", s.taskMap)
	t, ok := s.taskMap[id]
	if !ok {
		fmt.Printf("task not found: %s\n", id)
		return
	}

	fmt.Printf("id: %v\n", id)
	fmt.Printf("t: %v\n", t)

	// 从映射中移除任务
	delete(s.taskMap, id)

	// 从最小堆中移除任务
	for i := 0; i < s.heap.Len(); i++ {
		if (*s.heap)[i].ID() == id {
			heap.Remove(s.heap, i)
			break
		}
	}

	t.SetStatus(task.TaskStatusSuspended)
	// 将任务保存到已取消任务映射中
	s.cancelledTasks[id] = t

	// 从Redis中删除任务
	if s.persistence != nil {
		s.persistence.SaveTask(t)
	}

}

// Resume 恢复已取消的任务
// id: 要恢复的任务ID
func (s *Scheduler) Resume(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var t task.Task

	// 从已取消任务映射中查找任务
	t, ok := s.cancelledTasks[id]
	if ok {
		// 从已取消任务映射中移除
		delete(s.cancelledTasks, id)
	}

	if s.persistence != nil {
		// 从Redis加载任务
		task, err := s.persistence.LoadTask(id)
		if err != nil {
			return
		}
		t = task
	}

	if t == nil {
		return
	}

	// 确保下次执行时间正确
	// 对于一次性任务，如果执行时间已过，使用当前时间
	// 对于周期任务，重新计算下次执行时间
	if t.NextExecTime() <= time.Now().UnixMilli() {
		if t.Type() == task.TaskTypeOnce {
			t.SetNextExecTime(time.Now().UnixMilli())
		} else {
			t.UpdateNextExecTime()
		}
	}
	// 设置任务状态为Pending
	t.SetStatus(task.TaskStatusPending)

	// 重新添加到任务队列
	s.taskMap[id] = t
	heap.Push(s.heap, t)

	// 保存到Redis持久化
	if s.persistence != nil {
		s.persistence.SaveTask(t)
	}
}

// ExecuteTaskImmediately 立即执行指定任务
// id: 要立即执行的任务ID
func (s *Scheduler) ExecuteTaskImmediately(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 查找任务
	t, ok := s.taskMap[id]
	if !ok {
		return fmt.Errorf("任务不存在或已取消")
	}

	// 检查任务状态是否为Pending或Running
	if t.Status() == task.TaskStatusRunning {
		return fmt.Errorf("任务状态Running，无法立即执行")
	}

	// 将任务的下次执行时间设置为当前时间，使其立即执行
	t.SetNextExecTime(time.Now().UnixMilli())

	// 重新调整堆结构，确保任务在堆顶
	// 先从堆中移除任务
	for i := 0; i < s.heap.Len(); i++ {
		if (*s.heap)[i].ID() == id {
			heap.Remove(s.heap, i)
			break
		}
	}
	// 重新添加任务到堆中
	heap.Push(s.heap, t)

	// 保存任务状态到Redis
	if s.persistence != nil {
		s.persistence.SaveTask(t)
	}

	return nil
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
			// 检查是否有任务需要执行
			s.mu.Lock()
			if s.heap.Len() == 0 {
				s.mu.Unlock()
				time.Sleep(200 * time.Millisecond)
				continue
			}

			// 获取堆顶任务（下次执行时间最早的任务）
			nextTask := s.heap.Peek()
			waitDur := nextTask.NextExecTime() - time.Now().UnixMilli()

			// 如果任务时间到了，执行任务
			if waitDur <= 0 {
				// 从堆中弹出任务
				execTask := heap.Pop(s.heap).(task.Task)
				s.mu.Unlock()

				// 提交任务到执行器异步执行
				go s.executeTask(execTask)
			} else {
				// 任务时间未到，需要等待
				taskID := nextTask.ID()
				s.mu.Unlock()

				// 使用较短的睡眠时间，定期检查堆顶是否有更紧急的任务
				const checkInterval = 50 * time.Millisecond

				// 持续检查直到时间到了或者有更紧急的任务
				for {
					// 等待检查间隔
					time.Sleep(checkInterval)

					// 检查是否有更紧急的任务
					s.mu.Lock()
					if s.heap.Len() == 0 {
						s.mu.Unlock()
						break
					}

					// 重新获取堆顶任务
					currentTopTask := s.heap.Peek()

					// 检查当前堆顶任务是否与之前的相同
					if currentTopTask.ID() != taskID {
						// 堆顶任务已变化，说明有更紧急的任务，跳出循环重新检查
						s.mu.Unlock()
						break
					}
					// 检查任务时间是否到了
					if currentTopTask.NextExecTime() <= time.Now().UnixMilli() {
						// 任务时间到了，跳出循环执行
						s.mu.Unlock()
						break
					}
					s.mu.Unlock()
				}
			}
		}
	}
}

// executeTask 执行任务
// t: 要执行的任务
func (s *Scheduler) executeTask(t task.Task) {
	// 检查任务状态，只有Pending状态的任务才能执行
	if t.Status() != task.TaskStatusPending {
		// 任务状态不是Pending，对于周期性任务需要重新添加到堆中
		s.resetCronTask(t)
		return
	}

	// 尝试获取分布式锁
	if s.persistence != nil {
		// 设置锁过期时间为任务超时时间的2倍，确保任务有足够时间执行
		lockExpire := t.Timeout() * 2
		if lockExpire <= 0 {
			// 如果任务没有超时设置，默认锁过期时间为3分钟
			lockExpire = 3 * time.Minute
		}

		// 获取分布式锁
		hasLock, err := s.persistence.AcquireLock(t.ID(), lockExpire, s.nodeFlag)
		if err != nil {
			// 锁获取失败，记录日志或进行其他处理
			log.Printf("Failed to acquire lock for task %s: %v", t.ID(), err)
			// 对于周期性任务需要重新添加到堆中
			s.resetCronTask(t)
			return
		}

		if !hasLock {
			// 没有获取到锁，说明其他机器正在执行该任务
			// 对于周期性任务需要重新添加到堆中
			s.resetCronTask(t)
			return
		}

		// 确保无论任务执行成功与否，都释放锁
		defer func() {
			s.persistence.ReleaseLock(t.ID(), s.nodeFlag)
		}()
	}

	// 更新任务状态为Running
	t.SetStatus(task.TaskStatusRunning)

	// 保存任务状态到Redis
	if s.persistence != nil {
		s.persistence.SaveTask(t)
	}

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

	// 记录执行开始时间
	startTime := time.Now().UnixMilli()

	// 提交到异步执行器
	err := s.executor.Submit(ctx, t, func(execErr error) {
		// 计算执行耗时
		duration := time.Now().UnixMilli() - startTime
		// 任务完成后取消上下文
		defer cancel()
		// 处理任务执行结果
		s.handleTaskResult(t, execErr, startTime, time.Duration(duration))
	})

	if err != nil {
		// 计算执行耗时
		duration := time.Now().UnixMilli() - startTime
		// 提交任务失败
		cancel()
		s.handleTaskResult(t, err, startTime, time.Duration(duration))
	}
}

// 周期性任务重新入堆
func (s *Scheduler) resetCronTask(t task.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查任务是否已被取消
	if _, isCancelled := s.cancelledTasks[t.ID()]; isCancelled {
		// 任务已被取消，不再重新入堆
		return
	}

	if t.Type() == task.TaskTypeCron {
		t.ResetRetry()
		t.SetStatus(task.TaskStatusPending)
		t.UpdateNextExecTime()
		s.taskMap[t.ID()] = t
		heap.Push(s.heap, t)
		if s.persistence != nil {
			s.persistence.SaveTask(t)
		}
	}
}

// handleTaskResult 处理任务执行结果
// t: 执行的任务
// execErr: 执行错误，nil表示成功
// startTime: 执行开始时间
// duration: 执行耗时
func (s *Scheduler) handleTaskResult(t task.Task, execErr error, startTime int64, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查任务是否已被取消
	if _, isCancelled := s.cancelledTasks[t.ID()]; isCancelled {
		// 任务已被取消，不再处理执行结果
		return
	}

	// 失败重试逻辑
	if execErr != nil {
		// 创建失败记录
		failureRecord := task.FailureRecord{
			ID:        uuid.New().String(),
			TaskID:    t.ID(),
			Params:    t.Params(),
			ExecTime:  startTime,
			Duration:  duration,
			Error:     execErr.Error(),
			CreatedAt: time.Now().UnixMilli(),
		}

		// 保存失败记录到Redis
		if s.persistence != nil {
			s.persistence.SaveFailureRecord(failureRecord)
		} else {
			// 保存失败记录到内存
			s.failureRecords[t.ID()] = append(s.failureRecords[t.ID()], failureRecord)
		}

		if t.RetryPolicy().MaxRetry > t.RetryPolicy().CurrentRetry {
			// 增加重试次数
			t.RetryPolicy().CurrentRetry++
			// 计算重试执行时间（当前时间+重试间隔）
			retryTime := time.Now().Add(t.RetryPolicy().RetryDelay).UnixMilli()
			t.SetNextExecTime(retryTime)
			// 重置任务状态为Pending，准备重试
			t.SetStatus(task.TaskStatusPending)
			// 重新添加到任务队列
			s.taskMap[t.ID()] = t
			heap.Push(s.heap, t)
			// 更新Redis中的任务状态
			if s.persistence != nil {
				s.persistence.SaveTask(t)
			}
			return
		}
	}

	// 成功重置重试次数
	t.ResetRetry()

	// 周期任务, 计算下次执行时间
	if t.Type() == task.TaskTypeCron {
		t.UpdateNextExecTime()
		// 重置任务状态为Pending，准备下次执行
		t.SetStatus(task.TaskStatusPending)
		// 重新添加到任务队列
		s.taskMap[t.ID()] = t
		heap.Push(s.heap, t)

		// 保存完成状态到Redis
		if s.persistence != nil {
			s.persistence.SaveTask(t)
		}
	} else if t.Type() == task.TaskTypeOnce {
		// 一次性任务执行完成， 从任务映射中移除
		delete(s.taskMap, t.ID())

		// redis中删除任务
		if s.persistence != nil {
			s.persistence.DeleteTask(t.ID())
		}
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

// GetFailureRecords 获取指定任务的失败记录
// taskID: 任务ID
func (s *Scheduler) GetFailureRecords(taskID string) []task.FailureRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 先从内存获取失败记录
	records := s.failureRecords[taskID]

	// 如果有Redis持久化，从Redis加载最新的失败记录
	if s.persistence != nil {
		redisRecords, err := s.persistence.LoadFailureRecords(taskID)
		if err == nil && len(redisRecords) > 0 {
			records = redisRecords
		}
	}

	return records
}

// GetAllFailureRecords 获取所有任务的失败记录
func (s *Scheduler) GetAllFailureRecords() map[string][]task.FailureRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 如果有Redis持久化，从Redis加载所有失败记录
	if s.persistence != nil {
		allRecords, err := s.persistence.LoadAllFailureRecords()
		if err == nil && len(allRecords) > 0 {
			return allRecords
		}
	}

	return s.failureRecords
}

// RetryFailedTask 基于失败记录重试任务
// taskID: 任务ID
// recordID: 失败记录ID，为空则使用最新的失败记录
func (s *Scheduler) RetryFailedTask(taskID string, recordID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 获取失败记录
	var targetRecord *task.FailureRecord

	// 优先从Redis获取最新的失败记录
	if s.persistence != nil {
		records, err := s.persistence.LoadFailureRecords(taskID)
		if err == nil && len(records) > 0 {
			if recordID != "" {
				// 查找指定的失败记录
				for i := range records {
					if records[i].ID == recordID {
						targetRecord = &records[i]
						break
					}
				}
			} else {
				// 使用最新的失败记录（列表末尾）
				targetRecord = &records[len(records)-1]
			}
		}
	}

	// 如果Redis中没有找到，从内存中查找
	if targetRecord == nil {
		records, exists := s.failureRecords[taskID]
		if !exists || len(records) == 0 {
			return fmt.Errorf("no failure records found for task %s", taskID)
		}

		if recordID != "" {
			// 查找指定的失败记录
			for i := range records {
				if records[i].ID == recordID {
					targetRecord = &records[i]
					break
				}
			}
		} else {
			// 使用最新的失败记录
			targetRecord = &records[len(records)-1]
		}
	}

	if targetRecord == nil {
		return fmt.Errorf("failure record not found: taskID=%s, recordID=%s", taskID, recordID)
	}

	// 查找任务
	t, exists := s.taskMap[taskID]
	if !exists {
		// 任务不存在，尝试从已取消任务中查找
		t, exists = s.cancelledTasks[taskID]
		if !exists {
			// 有redis持久化层，尝试从Redis加载任务
			if s.persistence != nil {
				task, err := s.persistence.LoadTask(taskID)
				if err != nil {
					return fmt.Errorf("failed to load task from persistence: %w", err)
				}
				t = task
			} else {
				return fmt.Errorf("task not found: %s", taskID)
			}
		}
	}

	// 重置任务状态和重试次数
	t.SetStatus(task.TaskStatusPending)
	t.ResetRetry()

	// 立即执行任务
	t.SetNextExecTime(time.Now().UnixMilli())

	// 如果任务已取消，需要从取消列表中移除并重新添加到任务队列
	if _, exists := s.cancelledTasks[taskID]; exists {
		delete(s.cancelledTasks, taskID)
		s.taskMap[taskID] = t
		heap.Push(s.heap, t)

		// 保存任务状态到Redis
		if s.persistence != nil {
			s.persistence.SaveTask(t)
		}
	} else if !exists {
		// 任务是新创建的，添加到任务队列
		s.taskMap[taskID] = t
		heap.Push(s.heap, t)

		// 保存任务状态到Redis
		if s.persistence != nil {
			s.persistence.SaveTask(t)
		}
	}

	return nil
}

// DeleteFailureRecord 删除指定的失败记录
// taskID: 任务ID
// recordID: 失败记录ID
func (s *Scheduler) DeleteFailureRecord(taskID string, recordID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 从Redis中删除
	if s.persistence != nil {
		err := s.persistence.DeleteFailureRecord(taskID, recordID)
		if err != nil {
			return err
		}
	}

	// 从内存中删除
	if records, exists := s.failureRecords[taskID]; exists {
		var remainingRecords []task.FailureRecord
		for _, record := range records {
			if record.ID != recordID {
				remainingRecords = append(remainingRecords, record)
			}
		}
		if len(remainingRecords) > 0 {
			s.failureRecords[taskID] = remainingRecords
		} else {
			delete(s.failureRecords, taskID)
		}
	}

	return nil
}

// DeleteAllFailureRecords 删除指定任务的所有失败记录
// taskID: 任务ID
func (s *Scheduler) DeleteAllFailureRecords(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 从Redis中删除
	if s.persistence != nil {
		err := s.persistence.DeleteAllFailureRecords(taskID)
		if err != nil {
			return err
		}
	}

	// 从内存中删除
	delete(s.failureRecords, taskID)

	return nil
}

// GetAllTasks 获取所有任务，包括待执行、执行中、已完成和已取消的任务
func (s *Scheduler) GetAllTasks() []task.Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 收集所有任务
	tasks := make([]task.Task, 0, len(s.taskMap)+len(s.cancelledTasks))

	// 添加活跃任务
	for _, t := range s.taskMap {
		tasks = append(tasks, t)
	}

	// 添加已取消任务
	for _, t := range s.cancelledTasks {
		tasks = append(tasks, t)
	}

	// 如果有持久化层，从Redis加载所有任务，确保获取完整的任务列表
	if s.persistence != nil {
		redisTasks, err := s.persistence.LoadAllTasks()
		if err == nil {
			// 使用map去重
			taskMap := make(map[string]task.Task)
			for _, t := range tasks {
				taskMap[t.ID()] = t
			}
			for _, t := range redisTasks {
				if _, exists := taskMap[t.ID()]; !exists {
					tasks = append(tasks, t)
				}
			}
		}
	}

	return tasks
}

// GetTasksByStatus 根据状态获取任务
// status: 任务状态
func (s *Scheduler) GetTasksByStatus(status task.TaskStatus) []task.Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 收集符合状态的任务
	var tasks []task.Task

	// 检查活跃任务
	for _, t := range s.taskMap {
		if t.Status() == status {
			tasks = append(tasks, t)
		}
	}

	// 检查已取消任务
	for _, t := range s.cancelledTasks {
		if t.Status() == status {
			tasks = append(tasks, t)
		}
	}

	// 如果有持久化层，从Redis加载所有任务并过滤
	if s.persistence != nil {
		redisTasks, err := s.persistence.LoadAllTasks()
		if err == nil {
			// 使用map去重
			taskMap := make(map[string]task.Task)
			for _, t := range tasks {
				taskMap[t.ID()] = t
			}
			for _, t := range redisTasks {
				if t.Status() == status {
					if _, exists := taskMap[t.ID()]; !exists {
						tasks = append(tasks, t)
					}
				}
			}
		}
	}

	return tasks
}

// GetTask 根据ID获取任务
// id: 任务ID
func (s *Scheduler) GetTask(id string) task.Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 先从活跃任务中查找
	if t, exists := s.taskMap[id]; exists {
		return t
	}

	// 再从已取消任务中查找
	if t, exists := s.cancelledTasks[id]; exists {
		return t
	}

	// 如果有持久化层，从Redis中查找
	if s.persistence != nil {
		t, err := s.persistence.LoadTask(id)
		if err == nil {
			return t
		}
	}

	return nil
}
