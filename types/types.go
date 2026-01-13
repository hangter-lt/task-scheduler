package types

import "context"

// TaskType 任务类型枚举
type TaskType string

const (
	TaskTypeOnce TaskType = "once" // 一次性任务，只执行一次
	TaskTypeCron TaskType = "cron" // Cron任务，按表达式周期性执行
)

// HandleFunc 任务执行函数类型
type HandleFunc func(ctx context.Context, params map[string]any) error
