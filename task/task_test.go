package task

import (
	"context"
	"testing"
	"time"
)

// 测试用的函数定义
var (
	called         bool
	receivedParams map[string]any
	callCount      int

	// 注册测试函数
	testFuncID1 FuncID = "test-func-1"
	testFuncID2 FuncID = "test-func-2"
	testFuncID3 FuncID = "test-func-3"
)

// 初始化函数，注册测试函数
func init() {
	RegisterFunc(testFuncID1, func(ctx context.Context, params map[string]any) error {
		called = true
		receivedParams = params
		return nil
	})

	RegisterFunc(testFuncID2, func(ctx context.Context, params map[string]any) error {
		called = true
		receivedParams = params
		return nil
	})

	RegisterFunc(testFuncID3, func(ctx context.Context, params map[string]any) error {
		callCount++
		receivedParams = params
		return nil
	})
}

func TestBaseTask(t *testing.T) {
	// 创建重试策略
	retryPolicy := &RetryPolicy{
		MaxRetry:     3,
		RetryDelay:   time.Second,
		CurrentRetry: 1,
	}

	// 创建基础任务
	baseTask := BaseTask{
		id:           "test-task",
		timeout:      time.Second * 10,
		retryPolicy:  retryPolicy,
		nextExecTime: time.Now().Add(time.Minute),
		taskType:     TaskTypeOnce,
	}

	// 测试 ID() 方法
	if baseTask.ID() != "test-task" {
		t.Errorf("Expected task ID 'test-task', got '%s'", baseTask.ID())
	}

	// 测试 Type() 方法
	if baseTask.Type() != TaskTypeOnce {
		t.Errorf("Expected task type 'once', got '%s'", baseTask.Type())
	}

	// 测试 Timeout() 方法
	if baseTask.Timeout() != time.Second*10 {
		t.Errorf("Expected timeout 10s, got %v", baseTask.Timeout())
	}

	// 测试 RetryPolicy() 方法
	if baseTask.RetryPolicy() != retryPolicy {
		t.Errorf("Expected retry policy to match")
	}

	// 测试 ResetRetry() 方法
	baseTask.ResetRetry()
	if baseTask.RetryPolicy().CurrentRetry != 0 {
		t.Errorf("Expected CurrentRetry to be 0 after reset, got %d", baseTask.RetryPolicy().CurrentRetry)
	}

	// 测试 SetNextExecTime() 和 NextExecTime() 方法
	newTime := time.Now().Add(time.Hour)
	baseTask.SetNextExecTime(newTime)
	if !baseTask.NextExecTime().Equal(newTime) {
		t.Errorf("Expected next exec time to be %v, got %v", newTime, baseTask.NextExecTime())
	}
}

func TestOnceTask(t *testing.T) {
	// 重置测试标志
	called = false
	receivedParams = nil

	// 创建测试参数
	testParams := map[string]any{"name": "test-task", "value": 123}

	// 创建一次性任务
	execTime := time.Now().Add(time.Second)
	onceTask := NewOnceTask(
		"once-1",
		execTime,
		time.Second*5,
		&RetryPolicy{MaxRetry: 2, RetryDelay: time.Second},
		testFuncID1,
		testParams,
	)

	// 验证任务属性
	if onceTask.ID() != "once-1" {
		t.Errorf("Expected task ID 'once-1', got '%s'", onceTask.ID())
	}

	if onceTask.Type() != TaskTypeOnce {
		t.Errorf("Expected task type 'once', got '%s'", onceTask.Type())
	}

	if !onceTask.NextExecTime().Equal(execTime) {
		t.Errorf("Expected exec time %v, got %v", execTime, onceTask.NextExecTime())
	}

	// 测试 Run() 方法
	if err := onceTask.Run(context.Background()); err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !called {
		t.Error("Expected task function to be called")
	}

	// 测试参数传递
	if receivedParams == nil {
		t.Error("Expected params to be passed to task function")
	} else {
		if receivedParams["name"] != "test-task" {
			t.Errorf("Expected param 'name' to be 'test-task', got %v", receivedParams["name"])
		}
		if receivedParams["value"] != 123 {
			t.Errorf("Expected param 'value' to be 123, got %v", receivedParams["value"])
		}
	}

	// 测试 UpdateNextExecTime() 方法
	onceTask.UpdateNextExecTime()
	if !onceTask.NextExecTime().IsZero() {
		t.Error("Expected next exec time to be zero after UpdateNextExecTime()")
	}
}

func TestNewImmediateTask(t *testing.T) {
	// 重置测试标志
	called = false
	receivedParams = nil

	// 创建测试参数
	testParams := map[string]any{"name": "immediate-task", "value": 456}

	// 创建立即执行任务
	immediateTask := NewImmediateTask(
		"immediate-1",
		time.Second*5,
		&RetryPolicy{MaxRetry: 1, RetryDelay: time.Second},
		testFuncID2,
		testParams,
	)

	// 验证任务属性
	if immediateTask.ID() != "immediate-1" {
		t.Errorf("Expected task ID 'immediate-1', got '%s'", immediateTask.ID())
	}

	if immediateTask.Type() != TaskTypeOnce {
		t.Errorf("Expected task type 'once', got '%s'", immediateTask.Type())
	}

	// 验证立即执行任务的下次执行时间应该接近当前时间
	if immediateTask.NextExecTime().After(time.Now().Add(time.Millisecond * 100)) {
		t.Error("Expected immediate task to have exec time close to now")
	}

	// 测试 Run() 方法
	if err := immediateTask.Run(context.Background()); err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !called {
		t.Error("Expected immediate task function to be called")
	}

	// 测试参数传递
	if receivedParams == nil {
		t.Error("Expected params to be passed to immediate task function")
	} else {
		if receivedParams["name"] != "immediate-task" {
			t.Errorf("Expected param 'name' to be 'immediate-task', got %v", receivedParams["name"])
		}
		if receivedParams["value"] != 456 {
			t.Errorf("Expected param 'value' to be 456, got %v", receivedParams["value"])
		}
	}
}

func TestCronTask(t *testing.T) {
	// 重置测试标志
	callCount = 0
	receivedParams = nil

	// 创建测试参数
	testParams := map[string]any{"name": "cron-task", "value": 789}

	// 创建周期性任务（每秒执行一次）
	cronTask := NewCronTask(
		"cron-1",
		"*/1 * * * * *",
		time.Second*5,
		&RetryPolicy{MaxRetry: 2, RetryDelay: time.Second},
		testFuncID3,
		testParams,
	)

	// 验证任务属性
	if cronTask.ID() != "cron-1" {
		t.Errorf("Expected task ID 'cron-1', got '%s'", cronTask.ID())
	}

	if cronTask.Type() != TaskTypeCron {
		t.Errorf("Expected task type 'cron', got '%s'", cronTask.Type())
	}

	// 测试 Run() 方法
	if err := cronTask.Run(context.Background()); err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if callCount != 1 {
		t.Errorf("Expected task function to be called once, got %d calls", callCount)
	}

	// 测试参数传递
	if receivedParams == nil {
		t.Error("Expected params to be passed to cron task function")
	} else {
		if receivedParams["name"] != "cron-task" {
			t.Errorf("Expected param 'name' to be 'cron-task', got %v", receivedParams["name"])
		}
		if receivedParams["value"] != 789 {
			t.Errorf("Expected param 'value' to be 789, got %v", receivedParams["value"])
		}
	}

	// 测试 UpdateNextExecTime() 方法
	// 先获取初始执行时间
	initialTime := cronTask.NextExecTime()

	// 等待足够长的时间，确保秒数发生变化
	time.Sleep(time.Millisecond * 1500) // 等待1.5秒

	// 调用UpdateNextExecTime，应该得到新的执行时间
	cronTask.UpdateNextExecTime()
	newTime := cronTask.NextExecTime()

	// 验证新时间确实比初始时间晚
	if !newTime.After(initialTime) {
		t.Errorf("Expected new exec time to be after initial time. Initial: %v, New: %v", initialTime, newTime)
	}
}

func TestTaskWithNilParams(t *testing.T) {
	// 重置测试标志
	called = false

	// 创建没有参数的任务
	onceTask := NewOnceTask(
		"once-nil-params",
		time.Now().Add(time.Second),
		time.Second*5,
		nil,
		testFuncID1,
		nil, // 传递nil参数
	)

	// 测试 Run() 方法
	if err := onceTask.Run(context.Background()); err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !called {
		t.Error("Expected task function to be called")
	}
}

func TestTaskWithNilRetryPolicy(t *testing.T) {
	// 重置测试标志
	called = false

	// 创建没有重试策略的任务
	onceTask := NewOnceTask(
		"once-nil-retry",
		time.Now().Add(time.Second),
		time.Second*5,
		nil, // 传递nil重试策略
		testFuncID1,
		map[string]any{},
	)

	// 验证重试策略被初始化为默认值
	retryPolicy := onceTask.RetryPolicy()
	if retryPolicy == nil {
		t.Error("Expected retry policy to be initialized, got nil")
	} else {
		if retryPolicy.MaxRetry != 0 {
			t.Errorf("Expected default MaxRetry to be 0, got %d", retryPolicy.MaxRetry)
		}
		if retryPolicy.RetryDelay != 0 {
			t.Errorf("Expected default RetryDelay to be 0, got %v", retryPolicy.RetryDelay)
		}
		if retryPolicy.CurrentRetry != 0 {
			t.Errorf("Expected default CurrentRetry to be 0, got %d", retryPolicy.CurrentRetry)
		}
	}

	// 测试 Run() 方法
	if err := onceTask.Run(context.Background()); err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !called {
		t.Error("Expected task function to be called")
	}
}
