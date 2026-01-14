package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hangter-lt/task-scheduler/executor"
	"github.com/hangter-lt/task-scheduler/task"
)

func TestTaskHeap(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 创建多个任务，设置不同的执行时间
	task1 := task.NewOnceTask(
		"task-1",
		time.Now().Add(time.Second*3),
		time.Second*5,
		nil,
		func(ctx context.Context, params map[string]any) error {
			return nil
		},
		map[string]any{},
	)

	task2 := task.NewOnceTask(
		"task-2",
		time.Now().Add(time.Second*1),
		time.Second*5,
		nil,
		func(ctx context.Context, params map[string]any) error {
			return nil
		},
		map[string]any{},
	)

	task3 := task.NewOnceTask(
		"task-3",
		time.Now().Add(time.Second*2),
		time.Second*5,
		nil,
		func(ctx context.Context, params map[string]any) error {
			return nil
		},
		map[string]any{},
	)

	// 添加任务到堆
	heap.Push(scheduler.heap, task1)
	heap.Push(scheduler.heap, task2)
	heap.Push(scheduler.heap, task3)

	// 验证堆的大小
	if scheduler.heap.Len() != 3 {
		t.Errorf("Expected heap size 3, got %d", scheduler.heap.Len())
	}

	// 验证堆顶是执行时间最早的任务
	peekTask := scheduler.heap.Peek()
	if peekTask.ID() != "task-2" {
		t.Errorf("Expected heap top to be task-2, got %s", peekTask.ID())
	}

	// 测试弹出任务
	poppedTask := heap.Pop(scheduler.heap).(task.Task)
	if poppedTask.ID() != "task-2" {
		t.Errorf("Expected popped task to be task-2, got %s", poppedTask.ID())
	}

	// 验证新的堆顶
	peekTask = scheduler.heap.Peek()
	if peekTask.ID() != "task-3" {
		t.Errorf("Expected new heap top to be task-3, got %s", peekTask.ID())
	}
}

func TestSchedulerRegister(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 创建测试任务
	testTask := task.NewOnceTask(
		"register-test-task",
		time.Now().Add(time.Second),
		time.Second*5,
		nil,
		func(ctx context.Context, params map[string]any) error {
			return nil
		},
		map[string]any{},
	)

	// 注册任务
	scheduler.Register(testTask)

	// 验证任务是否被正确注册
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()

	if _, ok := scheduler.taskMap["register-test-task"]; !ok {
		t.Error("Expected task to be registered in taskMap")
	}

	if scheduler.heap.Len() != 1 {
		t.Errorf("Expected heap size 1, got %d", scheduler.heap.Len())
	}

	// 验证堆中的任务是我们注册的任务
	peekTask := scheduler.heap.Peek()
	if peekTask.ID() != "register-test-task" {
		t.Errorf("Expected heap top to be register-test-task, got %s", peekTask.ID())
	}
}

func TestSchedulerCancel(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 创建测试任务
	testTask := task.NewOnceTask(
		"cancel-test-task",
		time.Now().Add(time.Second*5), // 5s 后执行
		time.Second*5,
		nil,
		func(ctx context.Context, params map[string]any) error {
			return nil
		},
		map[string]any{},
	)

	// 注册任务
	scheduler.Register(testTask)

	// 取消任务
	scheduler.Cancel("cancel-test-task")

	// 验证任务是否被正确取消
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()

	if _, ok := scheduler.taskMap["cancel-test-task"]; ok {
		t.Error("Expected task to be removed from taskMap")
	}

	if scheduler.heap.Len() != 0 {
		t.Errorf("Expected heap size 0 after cancel, got %d", scheduler.heap.Len())
	}
}

func TestSchedulerOnceTaskExecution(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 测试标志
	var taskExecuted bool
	var wg sync.WaitGroup
	wg.Add(1)

	// 创建测试任务
	testTask := task.NewOnceTask(
		"once-execution-test",
		time.Now().Add(time.Millisecond*200), // 200ms 后执行
		0,
		nil,
		func(ctx context.Context, params map[string]any) error {
			defer wg.Done()
			taskExecuted = true
			return nil
		},
		map[string]any{},
	)

	// 注册任务
	scheduler.Register(testTask)

	// 启动调度器
	go scheduler.Run()

	// 等待任务执行
	wg.Wait()

	// 停止调度器
	scheduler.Stop()

	// 等待调度器完全停止
	time.Sleep(time.Millisecond * 300)

	// 验证任务是否执行
	if !taskExecuted {
		t.Error("Expected once task to be executed")
	}

	// 验证任务是否从调度器中移除
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()

	if _, ok := scheduler.taskMap["once-execution-test"]; ok {
		t.Error("Expected once task to be removed from taskMap after execution")
	}

	if scheduler.heap.Len() != 0 {
		t.Errorf("Expected heap size 0 after once task execution, got %d", scheduler.heap.Len())
	}
}

func TestSchedulerCronTaskExecution(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 测试标志
	var callCount int
	var wg sync.WaitGroup
	wg.Add(2) // 期望执行2次

	// 创建周期性任务（每100ms执行一次）
	cronTask := task.NewCronTask(
		"cron-execution-test",
		"*/1 * * * * *", // 每秒执行一次
		0,
		nil,
		func(ctx context.Context, params map[string]any) error {
			callCount++
			if callCount <= 2 {
				wg.Done()
			}
			return nil
		},
		map[string]any{},
	)

	// 注册任务
	scheduler.Register(cronTask)

	// 启动调度器
	go scheduler.Run()

	// 等待任务执行2次
	wg.Wait()

	// 停止调度器
	scheduler.Stop()

	// 等待调度器完全停止
	time.Sleep(time.Millisecond * 300)

	// 验证任务至少执行了2次
	if callCount < 2 {
		t.Errorf("Expected cron task to be executed at least 2 times, got %d calls", callCount)
	}

	// 验证调度器已停止
	if scheduler.IsRunning() {
		t.Error("Expected scheduler to be stopped")
	}
}

func TestSchedulerFailedTaskRecording(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 设置重试策略：最大重试2次，重试间隔100ms
	retryPolicy := &task.RetryPolicy{
		MaxRetry:   2,
		RetryDelay: time.Millisecond * 100,
	}

	// 测试标志
	var executionCount int

	// 创建测试任务，总是返回错误
	testTask := task.NewOnceTask(
		"failed-task-test",
		time.Now(), // 立即执行
		time.Second*5,
		retryPolicy,
		func(ctx context.Context, params map[string]any) error {
			executionCount++
			return errors.New("test task failed") // 总是返回错误
		},
		map[string]any{},
	)

	// 注册任务
	scheduler.Register(testTask)

	// 启动调度器
	go scheduler.Run()

	// 等待任务重试完成（重试2次 + 初始执行 = 3次执行）
	time.Sleep(time.Millisecond * 500)

	// 停止调度器
	scheduler.Stop()

	// 等待调度器完全停止
	time.Sleep(time.Millisecond * 300)

	// 验证任务执行了3次（初始执行 + 2次重试）
	if executionCount != 3 {
		t.Errorf("Expected task to be executed 3 times, got %d calls", executionCount)
	}

	// 验证任务被记录为失败任务
	failedTask, ok := scheduler.GetFailedTask("failed-task-test")
	if !ok {
		t.Error("Expected failed task to be recorded in failedTasks")
	} else {
		if failedTask.ID() != "failed-task-test" {
			t.Errorf("Expected failed task ID to be 'failed-task-test', got %s", failedTask.ID())
		}
		// 验证重试次数已达到上限
		if failedTask.RetryPolicy().CurrentRetry != 2 {
			t.Errorf("Expected failed task to have CurrentRetry = 2, got %d", failedTask.RetryPolicy().CurrentRetry)
		}
	}

	// 验证通过GetAllFailedTasks可以获取到失败任务
	allFailedTasks := scheduler.GetAllFailedTasks()
	if len(allFailedTasks) != 1 {
		t.Errorf("Expected GetAllFailedTasks to return 1 task, got %d tasks", len(allFailedTasks))
	} else {
		if allFailedTasks[0].ID() != "failed-task-test" {
			t.Errorf("Expected failed task ID from GetAllFailedTasks to be 'failed-task-test', got %s", allFailedTasks[0].ID())
		}
	}

	// 验证任务已从活跃任务列表中移除
	scheduler.mu.Lock()
	if _, ok := scheduler.taskMap["failed-task-test"]; ok {
		t.Error("Expected failed task to be removed from taskMap")
	}
	if scheduler.heap.Len() != 0 {
		t.Errorf("Expected heap to be empty after task failed, got %d tasks in heap", scheduler.heap.Len())
	}
	scheduler.mu.Unlock()
}

func TestSchedulerRunAndStop(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 验证调度器初始状态
	if scheduler.IsRunning() {
		t.Error("Expected scheduler to be not running initially")
	}

	// 启动调度器
	go scheduler.Run()

	// 等待调度器启动
	time.Sleep(time.Millisecond * 100)

	// 验证调度器运行状态
	if !scheduler.IsRunning() {
		t.Error("Expected scheduler to be running after Run()")
	}

	// 停止调度器
	scheduler.Stop()

	// 等待调度器完全停止
	time.Sleep(time.Millisecond * 300)

	// 验证调度器停止状态
	if scheduler.IsRunning() {
		t.Error("Expected scheduler to be stopped after Stop()")
	}
}

func TestSchedulerTaskWithParams(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 测试标志
	var receivedParams map[string]any
	var wg sync.WaitGroup
	wg.Add(1)

	// 创建测试参数
	testParams := map[string]any{"name": "test-param-task", "value": 123}

	// 创建测试任务
	testTask := task.NewOnceTask(
		"param-test-task",
		time.Now().Add(time.Millisecond*200),
		0,
		nil,
		func(ctx context.Context, params map[string]any) error {
			defer wg.Done()
			receivedParams = params
			return nil
		},
		testParams,
	)

	// 注册任务
	scheduler.Register(testTask)

	// 启动调度器
	go scheduler.Run()

	// 等待任务执行
	wg.Wait()

	// 停止调度器
	scheduler.Stop()

	// 等待调度器完全停止
	time.Sleep(time.Millisecond * 300)

	// 验证参数是否正确传递
	if receivedParams == nil {
		t.Error("Expected params to be passed to task function")
	} else {
		if receivedParams["name"] != "test-param-task" {
			t.Errorf("Expected param 'name' to be 'test-param-task', got %v", receivedParams["name"])
		}
		if receivedParams["value"] != 123 {
			t.Errorf("Expected param 'value' to be 123, got %v", receivedParams["value"])
		}
	}
}

func TestSchedulerCronTaskResume(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 测试标志
	var callCount int
	var wg sync.WaitGroup
	wg.Add(2) // 期望执行2次（恢复前后各1次）

	// 创建周期性任务（每500ms执行一次）
	cronTask := task.NewCronTask(
		"cron-resume-test",
		"*/1 * * * * *", // 每秒执行一次
		0,
		nil,
		func(ctx context.Context, params map[string]any) error {
			callCount++
			if callCount <= 2 {
				wg.Done()
			}
			return nil
		},
		map[string]any{},
	)

	// 注册任务
	scheduler.Register(cronTask)

	// 启动调度器
	go scheduler.Run()

	// 等待任务执行1次
	time.Sleep(time.Second * 1)

	// 取消任务
	scheduler.Cancel("cron-resume-test")

	// 验证任务已被取消
	scheduler.mu.Lock()
	if _, ok := scheduler.taskMap["cron-resume-test"]; ok {
		scheduler.mu.Unlock()
		t.Error("Expected task to be removed from taskMap after cancel")
	}
	if _, ok := scheduler.cancelledTasks["cron-resume-test"]; !ok {
		scheduler.mu.Unlock()
		t.Error("Expected task to be in cancelledTasks after cancel")
	}
	scheduler.mu.Unlock()

	// 等待一段时间，确保任务不会执行
	time.Sleep(time.Second * 1)

	// 恢复任务
	scheduler.Resume("cron-resume-test")

	// 验证任务已被恢复
	scheduler.mu.Lock()
	if _, ok := scheduler.taskMap["cron-resume-test"]; !ok {
		scheduler.mu.Unlock()
		t.Error("Expected task to be in taskMap after resume")
	}
	if _, ok := scheduler.cancelledTasks["cron-resume-test"]; ok {
		scheduler.mu.Unlock()
		t.Error("Expected task to be removed from cancelledTasks after resume")
	}
	scheduler.mu.Unlock()

	// 等待任务执行第2次
	wg.Wait()

	// 停止调度器
	scheduler.Stop()

	// 等待调度器完全停止
	time.Sleep(time.Millisecond * 300)

	// 验证任务至少执行了2次
	if callCount < 2 {
		t.Errorf("Expected cron task to be executed at least 2 times, got %d calls", callCount)
	}

	// 验证调度器已停止
	if scheduler.IsRunning() {
		t.Error("Expected scheduler to be stopped")
	}
}

func TestSchedulerOnceTaskResume(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 测试标志
	var taskExecuted bool
	var wg sync.WaitGroup
	wg.Add(1)

	// 创建一次性任务（2秒后执行）
	onceTask := task.NewOnceTask(
		"once-resume-test",
		time.Now().Add(time.Second*2),
		0,
		nil,
		func(ctx context.Context, params map[string]any) error {
			defer wg.Done()
			taskExecuted = true
			return nil
		},
		map[string]any{},
	)

	// 注册任务
	scheduler.Register(onceTask)

	// 启动调度器
	go scheduler.Run()

	// 立即取消任务
	scheduler.Cancel("once-resume-test")

	// 验证任务已被取消
	scheduler.mu.Lock()
	if _, ok := scheduler.taskMap["once-resume-test"]; ok {
		scheduler.mu.Unlock()
		t.Error("Expected task to be removed from taskMap after cancel")
	}
	if _, ok := scheduler.cancelledTasks["once-resume-test"]; !ok {
		scheduler.mu.Unlock()
		t.Error("Expected task to be in cancelledTasks after cancel")
	}
	scheduler.mu.Unlock()

	// 等待一段时间，确保任务不会执行
	time.Sleep(time.Second * 1)

	// 恢复任务
	scheduler.Resume("once-resume-test")

	// 验证任务已被恢复
	scheduler.mu.Lock()
	if _, ok := scheduler.taskMap["once-resume-test"]; !ok {
		scheduler.mu.Unlock()
		t.Error("Expected task to be in taskMap after resume")
	}
	if _, ok := scheduler.cancelledTasks["once-resume-test"]; ok {
		scheduler.mu.Unlock()
		t.Error("Expected task to be removed from cancelledTasks after resume")
	}
	scheduler.mu.Unlock()

	// 等待任务执行
	wg.Wait()

	// 停止调度器
	scheduler.Stop()

	// 等待调度器完全停止
	time.Sleep(time.Millisecond * 300)

	// 验证任务已执行
	if !taskExecuted {
		t.Error("Expected once task to be executed after resume")
	}

	// 验证调度器已停止
	if scheduler.IsRunning() {
		t.Error("Expected scheduler to be stopped")
	}
}
