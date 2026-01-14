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
	execTime time.Time,
	timeout time.Duration,
	retryPolicy *RetryPolicy,
	funcID FuncID,
	params map[string]any,
) *OnceTask {
	if retryPolicy == nil {
		retryPolicy = &RetryPolicy{
			MaxRetry:   0,
			RetryDelay: 0,
		}
	}

	if params == nil {
		params = map[string]any{}
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
	return NewOnceTask(id, time.Now(), timeout, retryPolicy, funcID, params)
}

// 清空下次执行时间
func (o *OnceTask) UpdateNextExecTime() {
	o.SetNextExecTime(time.Time{})
}
