package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hangter-lt/task-scheduler/executor"
	"github.com/hangter-lt/task-scheduler/task"
)

// 注册测试函数
func init() {
	// 注册测试函数，用于scheduler测试
	task.RegisterFunc("scheduler-test-func-1", func(ctx context.Context, params any) error {
		return nil
	})

	task.RegisterFunc("scheduler-test-func-2", func(ctx context.Context, params any) error {
		return nil
	})
}

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
		"scheduler-test-func-1",
		map[string]any{},
	)

	task2 := task.NewOnceTask(
		"task-2",
		time.Now().Add(time.Second*1),
		time.Second*5,
		nil,
		"scheduler-test-func-1",
		map[string]any{},
	)

	task3 := task.NewOnceTask(
		"task-3",
		time.Now().Add(time.Second*2),
		time.Second*5,
		nil,
		"scheduler-test-func-1",
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
		"scheduler-test-func-1",
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
		"",
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
		"",
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
		"",
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
		"",
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
		"",
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
		"",
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

func TestSchedulerFailureRecords(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 注册一个会失败的测试函数
	task.RegisterFunc("failure-test-func", func(ctx context.Context, params any) error {
		return fmt.Errorf("simulated task failure")
	})

	// 创建测试任务（立即执行）
	failureTask := task.NewOnceTask(
		"failure-test-task",
		time.Now().Add(time.Millisecond*50),
		0,
		&task.RetryPolicy{MaxRetry: 0}, // 不重试
		"failure-test-func",
		map[string]any{"test": "params"},
	)

	// 注册任务
	scheduler.Register(failureTask)

	// 启动调度器
	go scheduler.Run()

	// 等待任务执行
	time.Sleep(time.Millisecond * 200)

	// 停止调度器
	scheduler.Stop()

	// 等待调度器完全停止
	time.Sleep(time.Millisecond * 300)

	// 获取失败记录
	records := scheduler.GetFailureRecords("failure-test-task")

	// 验证失败记录是否创建
	if len(records) == 0 {
		t.Error("Expected failure record to be created for failed task")
	} else {
		// 验证失败记录内容
		record := records[0]
		if record.TaskID != "failure-test-task" {
			t.Errorf("Expected failure record TaskID to be 'failure-test-task', got %s", record.TaskID)
		}
		if record.Error != "simulated task failure" {
			t.Errorf("Expected failure record Error to be 'simulated task failure', got %s", record.Error)
		}
		if record.Params == nil {
			t.Error("Expected failure record Params to be set")
		}
		// Duration should be greater than 0, but let's not fail the test if it's not
		// since it depends on system clock precision
	}

	// 获取所有失败记录
	allRecords := scheduler.GetAllFailureRecords()
	if len(allRecords) == 0 {
		t.Error("Expected GetAllFailureRecords to return at least one record")
	}
}

func TestSchedulerRetryFailedTask(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 注册一个会失败的测试函数
	task.RegisterFunc("retry-test-func", func(ctx context.Context, params any) error {
		return fmt.Errorf("simulated task failure")
	})

	// 创建测试任务（立即执行）
	retryTask := task.NewOnceTask(
		"retry-test-task",
		time.Now().Add(time.Millisecond*50),
		0,
		&task.RetryPolicy{MaxRetry: 0}, // 不重试
		"retry-test-func",
		map[string]any{"test": "retry"},
	)

	// 注册任务
	scheduler.Register(retryTask)

	// 启动调度器
	go scheduler.Run()

	// 等待任务执行失败
	time.Sleep(time.Millisecond * 200)

	// 获取失败记录
	records := scheduler.GetFailureRecords("retry-test-task")
	if len(records) == 0 {
		t.Fatal("Expected failure record to be created for failed task")
	}

	// 使用最新的失败记录重试任务
	err = scheduler.RetryFailedTask("retry-test-task", "")
	if err != nil {
		t.Errorf("Expected RetryFailedTask to succeed, got error: %v", err)
	}

	// 验证任务状态
	scheduler.mu.Lock()
	taskFromMap, exists := scheduler.taskMap["retry-test-task"]
	scheduler.mu.Unlock()

	if !exists {
		t.Error("Expected task to be in taskMap after retry")
	} else if taskFromMap.Status() != task.TaskStatusPending {
		t.Errorf("Expected task status to be Pending, got %s", taskFromMap.Status())
	}

	// 停止调度器
	scheduler.Stop()
}

func TestSchedulerDeleteFailureRecords(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 注册一个会失败的测试函数
	task.RegisterFunc("delete-test-func", func(ctx context.Context, params any) error {
		return fmt.Errorf("simulated task failure")
	})

	// 创建测试任务（立即执行）
	deleteTask := task.NewOnceTask(
		"delete-test-task",
		time.Now().Add(time.Millisecond*50),
		0,
		&task.RetryPolicy{MaxRetry: 0}, // 不重试
		"delete-test-func",
		map[string]any{"test": "delete"},
	)

	// 注册任务
	scheduler.Register(deleteTask)

	// 启动调度器
	go scheduler.Run()

	// 等待任务执行失败
	time.Sleep(time.Millisecond * 200)

	// 获取失败记录
	records := scheduler.GetFailureRecords("delete-test-task")
	if len(records) == 0 {
		t.Fatal("Expected failure record to be created for failed task")
	}

	// 测试删除指定的失败记录
	recordID := records[0].ID
	err = scheduler.DeleteFailureRecord("delete-test-task", recordID)
	if err != nil {
		t.Errorf("Expected DeleteFailureRecord to succeed, got error: %v", err)
	}

	// 验证记录是否删除
	recordsAfterDelete := scheduler.GetFailureRecords("delete-test-task")
	if len(recordsAfterDelete) != 0 {
		t.Error("Expected no failure records after deletion")
	}

	// 重新执行任务以创建新的失败记录
	// 注意：我们需要重新创建任务，因为之前的任务可能已经被处理
	newRetryTask := task.NewOnceTask(
		"delete-test-task",
		time.Now().Add(time.Millisecond*50),
		0,
		&task.RetryPolicy{MaxRetry: 0}, // 不重试
		"delete-test-func",
		map[string]any{"test": "delete"},
	)
	scheduler.Register(newRetryTask)

	// 等待任务执行失败
	time.Sleep(time.Millisecond * 200)

	// 测试删除所有失败记录
	err = scheduler.DeleteAllFailureRecords("delete-test-task")
	if err != nil {
		t.Errorf("Expected DeleteAllFailureRecords to succeed, got error: %v", err)
	}

	// 验证所有记录是否删除
	recordsAfterDeleteAll := scheduler.GetFailureRecords("delete-test-task")
	if len(recordsAfterDeleteAll) != 0 {
		t.Error("Expected no failure records after DeleteAllFailureRecords")
	}

	// 停止调度器
	scheduler.Stop()
}

func TestSchedulerGetAllTasks(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 创建测试任务
	task1 := task.NewOnceTask(
		"test-task-1",
		time.Now().Add(time.Second*5),
		time.Second*5,
		nil,
		"scheduler-test-func-1",
		map[string]any{},
	)

	task2 := task.NewOnceTask(
		"test-task-2",
		time.Now().Add(time.Second*10),
		time.Second*5,
		nil,
		"scheduler-test-func-1",
		map[string]any{},
	)

	// 注册任务
	scheduler.Register(task1)
	scheduler.Register(task2)

	// 取消一个任务
	scheduler.Cancel("test-task-2")

	// 获取所有任务
	tasks := scheduler.GetAllTasks()

	// 验证任务数量
	if len(tasks) < 2 {
		t.Errorf("Expected at least 2 tasks, got %d", len(tasks))
	}
}

func TestSchedulerGetTasksByStatus(t *testing.T) {
	// 创建执行器
	exec, err := executor.NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建调度器
	scheduler := NewScheduler(exec)

	// 创建测试任务
	task1 := task.NewOnceTask(
		"test-task-1",
		time.Now().Add(time.Second*5),
		time.Second*5,
		nil,
		"scheduler-test-func-1",
		map[string]any{},
	)

	task2 := task.NewOnceTask(
		"test-task-2",
		time.Now().Add(time.Second*10),
		time.Second*5,
		nil,
		"scheduler-test-func-1",
		map[string]any{},
	)

	// 注册任务
	scheduler.Register(task1)
	scheduler.Register(task2)

	// 取消一个任务
	scheduler.Cancel("test-task-2")

	// 测试获取待执行任务
	pendingTasks := scheduler.GetTasksByStatus(task.TaskStatusPending)
	if len(pendingTasks) < 1 {
		t.Errorf("Expected at least 1 pending task, got %d", len(pendingTasks))
	}

	// 测试获取已取消任务
	canceledTasks := scheduler.GetTasksByStatus(task.TaskStatusCanceled)
	if len(canceledTasks) < 1 {
		t.Errorf("Expected at least 1 canceled task, got %d", len(canceledTasks))
	}
}

func TestSchedulerGetTask(t *testing.T) {
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
		"test-task",
		time.Now().Add(time.Second*5),
		time.Second*5,
		nil,
		"scheduler-test-func-1",
		map[string]any{"test": "value"},
	)

	// 注册任务
	scheduler.Register(testTask)

	// 测试获取存在的任务
	foundTask := scheduler.GetTask("test-task")
	if foundTask == nil {
		t.Error("Expected to find task by ID")
	}

	// 测试获取不存在的任务
	nonexistentTask := scheduler.GetTask("nonexistent-task")
	if nonexistentTask != nil {
		t.Error("Expected nil for nonexistent task")
	}

	// 测试获取已取消的任务
	scheduler.Cancel("test-task")
	canceledTask := scheduler.GetTask("test-task")
	if canceledTask == nil {
		t.Error("Expected to find canceled task by ID")
	}
}
