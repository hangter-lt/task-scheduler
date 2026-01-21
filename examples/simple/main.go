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
	// 注册任务函数
	task.RegisterFunc("example-func-1", func(ctx context.Context, params any) error {
		fmt.Printf("params: %v\n", params)
		return fmt.Errorf("simulated task failure")
	})

	exec, err := executor.NewExecutor(10)
	if err != nil {
		panic(err)
	}
	defer exec.Release()

	sch := scheduler.NewScheduler(exec)

	go sch.Run()

	// 指定时间的单次任务
	onceParams := map[string]any{"name": "once-task", "value": 1}
	once := task.NewOnceTask("1", time.Now().Add(time.Second*1), 0, nil, "example-func-1", onceParams)

	sch.Register(once)

	// 立即执行的单次任务 - 使用新方法
	immediateParams := map[string]any{"name": "immediate-task", "value": 3}
	immediate := task.NewImmediateTask("3", 0, nil, "example-func-1", immediateParams)

	sch.Register(immediate)

	// 每秒执行一次
	cronParams := map[string]any{"name": "cron-task", "value": 2}
	cron := task.NewCronTask("2", "*/1 * * * * *", time.Second*1, nil, "example-func-1", cronParams)
	sch.Register(cron)

	// 测试失败记录
	time.Sleep(time.Second * 10)
	records := sch.GetFailureRecords("2")
	// records := sch.GetAllFailureRecords()
	fmt.Printf("records: %v\n", records)

	select {}
}
