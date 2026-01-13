package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hangter-lt/task-scheduler/executor"
	"github.com/hangter-lt/task-scheduler/scheduler"
	"github.com/hangter-lt/task-scheduler/task"
)

func main() {

	exec, err := executor.NewExecutor(10)
	if err != nil {
		panic(err)
	}
	defer exec.Release()

	sch := scheduler.NewScheduler(exec)

	go sch.Run()

	// 指定时间的单次任务
	onceParams := map[string]any{"name": "once-task", "value": 1}
	once := task.NewOnceTask("1", time.Now().Add(time.Second*1), 0, nil, func(ctx context.Context, params map[string]any) error {
		fmt.Printf("指定时间单次任务执行: %v, 参数: %v\n", time.Now(), params)
		return nil
	}, onceParams)

	sch.Register(once)

	// 立即执行的单次任务 - 使用新方法
	immediateParams := map[string]any{"name": "immediate-task", "value": 2}
	immediate := task.NewImmediateTask("3", 0, nil, func(ctx context.Context, params map[string]any) error {
		fmt.Printf("立即执行单次任务执行: %v, 参数: %v\n", time.Now(), params)
		return nil
	}, immediateParams)

	sch.Register(immediate)

	// 每秒执行一次
	cronParams := map[string]any{"name": "cron-task", "value": 3}
	cron := task.NewCronTask("2", "*/1 * * * * *", time.Second*1, nil, func(ctx context.Context, params map[string]any) error {
		fmt.Printf("cron任务执行: %v, 参数: %v\n", time.Now(), params)
		return nil
	}, cronParams)
	sch.Register(cron)

	select {}
}
