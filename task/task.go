package task

import (
	"context"
	"fmt"
	"time"
)

// TaskType 任务类型枚举
type TaskType string

const (
	TaskTypeOnce TaskType = "once" // 一次性任务，只执行一次
	TaskTypeCron TaskType = "cron" // Cron任务，按表达式周期性执行
)

// TaskStatus 任务状态枚举
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"   // 待执行
	TaskStatusRunning   TaskStatus = "running"   // 执行中
	TaskStatusCompleted TaskStatus = "completed" // 已完成（一次性任务独有）
	TaskStatusCanceled  TaskStatus = "canceled"  // 已取消
)

// RetryPolicy 任务重试策略
type RetryPolicy struct {
	MaxRetry     int           // 最大重试次数，0表示不重试
	RetryDelay   time.Duration // 重试间隔
	CurrentRetry int           // 当前重试次数
}

// Task 任务接口，所有任务类型都必须实现此接口
type Task interface {
	ID() string                    // 获取任务ID
	Type() TaskType                // 获取任务类型(once/cron)
	Status() TaskStatus            // 获取任务状态
	NextExecTime() time.Time       // 获取下次执行时间
	Run(ctx context.Context) error // 执行任务
	Timeout() time.Duration        // 获取任务超时时间
	RetryPolicy() *RetryPolicy     // 获取重试策略
	SetNextExecTime(time.Time)     // 设置下次执行时间
	SetStatus(TaskStatus)          // 设置任务状态
	ResetRetry()                   // 重置重试次数
	UpdateNextExecTime()           // 更新下次执行时间（主要用于周期任务）
	FuncID() FuncID                // 获取任务执行函数标识符
	Params() any                   // 获取任务通用参数
	CronExpr() string              // 获取cron任务表达式
}

// BaseTask 任务基础结构，包含所有任务的通用字段
type BaseTask struct {
	id           string        // 任务唯一标识
	timeout      time.Duration // 任务超时时间
	retryPolicy  *RetryPolicy  // 重试策略
	nextExecTime time.Time     // 下次执行时间
	taskType     TaskType      // 任务类型
	funcID       FuncID        // 任务执行函数标识符
	params       any           // 任务通用参数
	status       TaskStatus    // 任务状态
}

// ID 获取任务ID
func (b *BaseTask) ID() string {
	return b.id
}

// Type 获取任务类型
func (b *BaseTask) Type() TaskType {
	return b.taskType
}

// NextExecTime 获取下次执行时间
func (b *BaseTask) NextExecTime() time.Time {
	return b.nextExecTime
}

// Timeout 获取任务超时时间
func (b *BaseTask) Timeout() time.Duration {
	return b.timeout
}

// RetryPolicy 获取重试策略
func (b *BaseTask) RetryPolicy() *RetryPolicy {
	return b.retryPolicy
}

// SetNextExecTime 设置下次执行时间
func (b *BaseTask) SetNextExecTime(t time.Time) {
	b.nextExecTime = t
}

// ResetRetry 重置重试次数
func (b *BaseTask) ResetRetry() {
	if b.retryPolicy != nil {
		b.retryPolicy.CurrentRetry = 0
	}
}

// UpdateNextExecTime 更新下次执行时间
// 基础任务不实现此方法，由具体任务类型实现
func (b *BaseTask) UpdateNextExecTime() {
}

// CronExpr 获取cron任务表达式
func (b *BaseTask) CronExpr() string {
	return ""
}

// FuncID 获取任务执行函数标识符
func (b *BaseTask) FuncID() FuncID {
	return b.funcID
}

// Params 获取任务通用参数
func (b *BaseTask) Params() any {
	return b.params
}

// Status 获取任务状态
func (b *BaseTask) Status() TaskStatus {
	return b.status
}

// SetStatus 设置任务状态
func (b *BaseTask) SetStatus(status TaskStatus) {
	b.status = status
}

// Run 执行任务
func (b *BaseTask) Run(ctx context.Context) error {
	f, ok := GetFunc(b.funcID)
	if !ok {
		return fmt.Errorf("task func not found: %s", b.funcID)
	}
	return f(ctx, b.params)
}
