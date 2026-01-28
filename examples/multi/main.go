package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/hangter-lt/task-scheduler/executor"
	"github.com/hangter-lt/task-scheduler/persistence"
	"github.com/hangter-lt/task-scheduler/scheduler"
	"github.com/hangter-lt/task-scheduler/task"
)

func main() {
	nodeFlag := os.Args[1]

	// 注册任务函数
	task.RegisterFunc("redis-example-func", func(ctx context.Context, params any) error {
		fmt.Printf("params: %v\n", params)
		fmt.Printf("time.Now(): %v\n", time.Now())
		return nil
	})

	// 创建执行器
	exec, err := executor.NewExecutor(10)
	if err != nil {
		panic(err)
	}
	defer exec.Release()

	// 创建Redis持久化层
	redisPersistence := persistence.NewRedisPersistence("localhost:6379", "", 2)

	// 创建带有Redis持久化的调度器
	sch := scheduler.NewSchedulerWithPersistence(exec, redisPersistence, nodeFlag)

	// 启动调度器
	go sch.Run()

	// 注册一个周期任务，每秒执行一次
	cronParams := map[string]any{"name": "redis-cron-task", "value": 1}
	cron := task.NewCronTask("redis-cron-1", "*/3 * * * * *", time.Minute*5, nil, "redis-example-func", cronParams)

	// 注册一个单次任务，当前时间5秒后执行
	onceParams := map[string]any{"name": "redis-once-task", "value": 2}
	// t, err := time.Parse(time.RFC3339, "2024-01-01T00:00:05Z")
	// if err != nil {
	// 	panic(err)
	// }
	once := task.NewOnceTask("redis-once-1", time.Now().Add(time.Second*5).UnixMilli(), time.Minute*1, nil, "redis-example-func", onceParams)

	// 注册任务
	sch.Register(cron)
	sch.Register(once)

	select {}
}
