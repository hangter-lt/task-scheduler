package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hangter-lt/task-scheduler/executor"
	"github.com/hangter-lt/task-scheduler/persistence"
	"github.com/hangter-lt/task-scheduler/scheduler"
	"github.com/hangter-lt/task-scheduler/task"
)

func main() {

	type TestParams struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	// 注册任务函数
	task.RegisterFunc("redis-example-func", func(ctx context.Context, params any) error {
		fmt.Printf("params: %v\n", params)
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
	sch := scheduler.NewSchedulerWithPersistence(exec, redisPersistence, "node-1")

	// 启动调度器
	go sch.Run()

	// 注册一个周期任务，每秒执行一次
	cronParams := TestParams{Name: "redis-cron-task", Value: 1}
	cron := task.NewCronTask("redis-cron-1", "*/3 * * * * *", time.Minute*5, nil, "redis-example-func", cronParams)

	// 注册一个单次任务，当前时间5秒后执行
	onceParams := map[string]any{"name": "redis-once-task", "value": 2}
	// t, err := time.Parse(time.RFC3339, "2024-01-01T00:00:05Z")
	// if err != nil {
	// 	panic(err)
	// }
	once := task.NewOnceTask("redis-once-1", time.Now().Add(time.Second*5), time.Minute*1, nil, "redis-example-func", onceParams)

	// 注册任务
	sch.Register(cron)
	sch.Register(once)

	fmt.Println("任务已注册，等待执行...")

	// 等待任务执行
	time.Sleep(time.Second * 5)

	// 取消周期任务
	fmt.Println("取消周期任务...")
	sch.Cancel("redis-cron-1")

	// 等待一段时间
	time.Sleep(time.Second * 10)

	// 恢复周期任务
	fmt.Println("恢复周期任务...")
	sch.Resume("redis-cron-1")

	// 无限等待，观察任务执行
	select {}
}
