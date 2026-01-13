package executor

import (
	"context"
	"fmt"

	"github.com/hangter-lt/task-scheduler/task"
	"github.com/panjf2000/ants/v2"
)

// Executor 任务执行器，管理任务执行的工作池
type Executor struct {
	pool *ants.Pool // 线程池，用于管理并发任务执行
}

// NewExecutor 创建一个新的任务执行器
// maxWorkers: 最大并发执行数
func NewExecutor(maxWorkers int) (*Executor, error) {
	pool, err := ants.NewPool(maxWorkers)
	if err != nil {
		return nil, err
	}
	return &Executor{pool: pool}, nil
}

// Submit 提交任务到执行器
// ctx: 上下文，用于控制任务执行
// t: 要执行的任务
// callback: 任务执行完成后的回调函数，返回执行结果
func (e *Executor) Submit(ctx context.Context, t task.Task, callback func(error)) error {
	return e.pool.Submit(func() {
		// 捕获异常
		var execErr error
		defer func() {
			if r := recover(); r != nil {
				execErr = fmt.Errorf("panic: %v", r)
			}
			callback(execErr)
		}()

		// 执行任务
		select {
		case <-ctx.Done():
			execErr = fmt.Errorf("context canceled: %w", ctx.Err())
			return
		default:
			execErr = t.Run(ctx)
		}
	})
}

// Release 释放执行器资源
func (e *Executor) Release() {
	e.pool.Release()
}
