package executor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hangter-lt/task-scheduler/task"
	"github.com/robfig/cron/v3"
)

func TestExecutor(t *testing.T) {
	// 创建执行器
	exec, err := NewExecutor(5)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 测试任务计数
	var wg sync.WaitGroup
	taskCount := 10
	wg.Add(taskCount)

	// 提交多个任务
	for i := 0; i < taskCount; i++ {
		// 创建测试任务
		testTask := &mockTask{
			id:      string(rune('a' + i)),
			timeout: time.Second,
			runFunc: func(ctx context.Context, params any) error {
				time.Sleep(time.Millisecond * 100)
				wg.Done()
				return nil
			},
		}

		// 提交任务
		err := exec.Submit(context.Background(), testTask, func(execErr error) {
			if execErr != nil {
				t.Errorf("Task %s failed: %v", testTask.ID(), execErr)
			}
		})

		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}
	}

	// 等待所有任务完成
	wg.Wait()
}

func TestExecutorTimeout(t *testing.T) {
	// 创建执行器
	exec, err := NewExecutor(1)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建一个会检查上下文的任务
	testTask := &mockTask{
		id:      "timeout-task",
		timeout: time.Millisecond * 100,
		runFunc: func(ctx context.Context, params any) error {
			// 在任务执行过程中检查上下文
			for i := 0; i < 20; i++ {
				select {
				case <-ctx.Done():
					return ctx.Err() // 返回上下文错误
				default:
					time.Sleep(time.Millisecond * 10) // 每次休眠10ms
				}
			}
			return nil
		},
	}

	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()

	// 提交任务
	var callbackCalled bool
	var execErr error

	err = exec.Submit(ctx, testTask, func(e error) {
		callbackCalled = true
		execErr = e
	})

	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// 等待任务执行完成
	time.Sleep(time.Millisecond * 200)

	// 验证回调被调用
	if !callbackCalled {
		t.Error("Expected callback to be called")
	}

	// 验证任务超时
	if execErr == nil {
		t.Error("Expected timeout error, got nil")
	} else {
		// 验证错误是上下文超时错误，不检查精确的错误信息格式
		expectedError := "context deadline exceeded"
		if execErr.Error() != expectedError {
			t.Errorf("Expected %q, got %q", expectedError, execErr.Error())
		}
	}
}

func TestExecutorPanic(t *testing.T) {
	// 创建执行器
	exec, err := NewExecutor(1)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建会panic的任务
	testTask := &mockTask{
		id:      "panic-task",
		timeout: time.Second,
		runFunc: func(ctx context.Context, params any) error {
			panic("test panic")
		},
	}

	// 提交任务
	var callbackCalled bool
	var execErr error

	err = exec.Submit(context.Background(), testTask, func(e error) {
		callbackCalled = true
		execErr = e
	})

	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// 等待任务执行完成
	time.Sleep(time.Millisecond * 100)

	// 验证回调被调用
	if !callbackCalled {
		t.Error("Expected callback to be called")
	}

	// 验证panic被捕获
	if execErr == nil {
		t.Error("Expected panic error, got nil")
	} else if execErr.Error() != "panic: test panic" {
		t.Errorf("Expected panic error, got %v", execErr)
	}
}

func TestExecutorContextCancel(t *testing.T) {
	// 创建执行器
	exec, err := NewExecutor(1)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 创建长时间运行的任务
	testTask := &mockTask{
		id:      "cancel-task",
		timeout: time.Second,
		runFunc: func(ctx context.Context, params any) error {
			time.Sleep(time.Second * 2) // 长时间运行
			return nil
		},
	}

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 提交任务
	var callbackCalled bool
	var execErr error

	err = exec.Submit(ctx, testTask, func(e error) {
		callbackCalled = true
		execErr = e
	})

	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// 立即取消上下文
	cancel()

	// 等待任务执行完成
	time.Sleep(time.Millisecond * 100)

	// 验证回调被调用
	if !callbackCalled {
		t.Error("Expected callback to be called")
	}

	// 验证上下文取消错误
	if execErr == nil {
		t.Error("Expected context canceled error, got nil")
	} else if execErr.Error() != "context canceled: context canceled" {
		t.Errorf("Expected context canceled error, got %v", execErr)
	}
}

func TestExecutorWithParams(t *testing.T) {
	// 创建执行器
	exec, err := NewExecutor(1)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Release()

	// 测试参数传递
	var receivedParams map[string]any
	var callbackCalled bool

	// 创建测试任务
	testTask := &mockTask{
		id:      "param-task",
		timeout: time.Second,
		runFunc: func(ctx context.Context, params any) error {
			receivedParams = params.(map[string]any)
			return nil
		},
	}

	// 提交任务
	err = exec.Submit(context.Background(), testTask, func(execErr error) {
		callbackCalled = true
		if execErr != nil {
			t.Errorf("Task failed: %v", execErr)
		}
	})

	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// 等待任务执行完成
	time.Sleep(time.Millisecond * 100)

	// 验证回调被调用
	if !callbackCalled {
		t.Error("Expected callback to be called")
	}

	// 验证参数传递（mockTask的params始终为空map）
	if receivedParams == nil {
		t.Error("Expected params to be passed, got nil")
	} else if len(receivedParams) != 0 {
		t.Errorf("Expected params to be empty, got %v", receivedParams)
	}
}

func TestExecutorRelease(t *testing.T) {
	// 创建执行器
	exec, err := NewExecutor(1)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	// 释放执行器
	exec.Release()

	// 尝试在释放后提交任务
	testTask := &mockTask{
		id:      "release-task",
		timeout: time.Second,
		runFunc: func(ctx context.Context, params any) error {
			return nil
		},
	}

	// 提交任务，应该失败
	err = exec.Submit(context.Background(), testTask, func(execErr error) {
	})

	// 验证任务提交失败
	if err == nil {
		t.Error("Expected task submission to fail after executor release")
	}
}

// mockTask 实现 task.Task 接口用于测试
type mockTask struct {
	id      string
	timeout time.Duration
	runFunc func(ctx context.Context, params any) error
}

func (m *mockTask) ID() string {
	return m.id
}

func (m *mockTask) Type() task.TaskType {
	return task.TaskTypeOnce
}

func (m *mockTask) NextExecTime() time.Time {
	return time.Now()
}

func (m *mockTask) Run(ctx context.Context) error {
	return m.runFunc(ctx, map[string]any{}) // 传递空参数
}

func (m *mockTask) Timeout() time.Duration {
	return m.timeout
}

func (m *mockTask) RetryPolicy() *task.RetryPolicy {
	return &task.RetryPolicy{MaxRetry: 0}
}

func (m *mockTask) SetNextExecTime(t time.Time) {}

func (m *mockTask) ResetRetry() {}

func (m *mockTask) UpdateNextExecTime() {}

func (m *mockTask) FuncID() task.FuncID {
	return task.FuncID("mock-run")
}

func (m *mockTask) Params() any {
	return nil
}

func (m *mockTask) CronExpr() string {
	return ""
}

func (m *mockTask) CronParser() *cron.Parser {
	return nil
}

func (m *mockTask) Status() task.TaskStatus {
	return task.TaskStatusPending
}

func (m *mockTask) SetStatus(status task.TaskStatus) {
	// 模拟实现，不做实际操作
}
