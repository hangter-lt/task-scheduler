package task

import (
	"time"
)

// 单性任务
type OnceTask struct {
	BaseTask
}

func NewOnceTask(
	id string,
	execTime int64,
	timeout time.Duration,
	retryPolicy *RetryPolicy,
	funcID FuncID,
	params any,
) *OnceTask {
	if retryPolicy == nil {
		retryPolicy = &RetryPolicy{
			MaxRetry:   0,
			RetryDelay: 0,
		}
	}

	return &OnceTask{
		BaseTask: BaseTask{
			id:           id,
			timeout:      timeout,
			retryPolicy:  retryPolicy,
			nextExecTime: execTime,
			taskType:     TaskTypeOnce,
			funcID:       funcID,
			params:       params,
			status:       TaskStatusPending,
		},
	}
}

// NewImmediateTask 创建一个立即执行的单次任务
func NewImmediateTask(
	id string,
	timeout time.Duration,
	retryPolicy *RetryPolicy,
	funcID FuncID,
	params map[string]any,
) *OnceTask {
	// 调用NewOnceTask，传入当前时间作为执行时间
	return NewOnceTask(id, time.Now().UnixMilli(), timeout, retryPolicy, funcID, params)
}

// 清空下次执行时间
func (o *OnceTask) UpdateNextExecTime() {
	o.SetNextExecTime(0)
}
