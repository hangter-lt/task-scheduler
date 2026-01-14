# task-scheduler

一个轻量级、高性能的Go语言任务调度系统，支持多种类型的任务调度，包括一次性任务、立即执行任务和Cron表达式任务。

## 项目特点

- 基于优先级队列的高效调度算法
- 支持多种任务类型（一次性、立即执行、Cron）
- 支持任务超时和自动重试
- 高并发设计，支持大量任务调度
- 易于集成到现有项目中

## 项目结构

```
├── executor/       # 执行器，负责执行任务
│   ├── executor.go
│   └── executor_test.go
├── examples/       # 示例代码
│   ├── main.go
├── scheduler/      # 调度器，负责调度任务
│   ├── heap.go
│   ├── scheduler.go
│   └── scheduler_test.go
├── task/           # 任务定义
│   ├── cron_task.go
│   ├── func.go        # 函数注册管理
│   ├── once_task.go
│   ├── task.go
│   └── task_test.go
├── go.mod
└── go.sum
```

## 核心组件

### 1. 执行器（Executor）

执行器负责管理任务执行的工作池，控制并发执行数量。

#### 主要功能
- 管理任务执行的工作池
- 支持动态调整并发执行数量
- 提供任务执行的生命周期管理
- 捕获任务执行过程中的异常

#### API
- `NewExecutor(maxWorkers int) (*Executor, error)` - 创建一个新的执行器
- `Submit(ctx context.Context, t task.Task, callback func(error)) error` - 提交任务到执行器
- `Release()` - 释放执行器资源

### 2. 调度器（Scheduler）

调度器负责管理和调度各种类型的任务，根据任务的下次执行时间进行排序和执行。

#### 主要功能
- 基于优先级队列的高效调度
- 支持任务的注册、取消和重新调度
- 自动处理任务的超时和重试
- 支持优雅关闭
- 支持Redis持久化

#### API
- `NewScheduler(exec *executor.Executor) *Scheduler` - 创建一个新的调度器
- `Register(t task.Task)` - 注册任务到调度器
- `Cancel(id string)` - 取消指定ID的任务
- `Resume(id string)` - 恢复已取消的任务
- `Run()` - 启动调度器
- `Stop()` - 停止调度器
- `IsRunning() bool` - 检查调度器是否正在运行
### 3. 任务类型

#### 一次性任务（OnceTask）

在指定时间点执行一次的任务。

- 支持延迟执行
- 执行完成后自动移除

#### 立即执行任务（ImmediateTask）

注册后立即执行的任务，是一次性任务的特殊形式。

- 适合紧急任务
- 执行完成后自动移除

#### Cron表达式任务（CronTask）

基于Cron表达式周期性执行的任务。

- 支持秒级精度
- 支持复杂的调度规则
- 执行完成后自动计算下次执行时间
### 4. 函数注册（Func Registry）

函数注册机制允许将任务执行函数注册到全局注册表中，通过函数ID引用，实现了任务定义与执行逻辑的解耦。

#### 主要功能
- 支持任务执行函数的注册和管理
- 通过函数ID引用执行函数
- 支持动态注册和获取

#### API
- `RegisterFunc(id FuncID, f HandleFunc)` - 注册任务执行函数
- `GetFunc(id FuncID) (HandleFunc, bool)` - 根据ID获取任务执行函数


## 快速开始

### 安装

```bash
go get -u github.com/hangter-lt/task-scheduler
```

### 运行示例

#### 基本示例
```bash
go run examples/main.go
```

### 基本使用

#### 1. 导入包

```go
import (
    "github.com/hangter-lt/task-scheduler/executor"
    "github.com/hangter-lt/task-scheduler/scheduler"
    "github.com/hangter-lt/task-scheduler/task"
)
```

#### 2. 创建执行器

```go
// 创建一个并发度为10的执行器
exec, err := executor.NewExecutor(10)
if err != nil {
    panic(err)
}
defer exec.Release()
```

#### 3. 创建调度器

```go
// 创建调度器，关联到执行器
sch := scheduler.NewScheduler(exec)

// 启动调度器
go sch.Run()
```

#### 4. 注册任务函数

```go
// 注册任务执行函数	
task.RegisterFunc("example-func-1", func(ctx context.Context, params map[string]any) error {
	name := params["name"].(string)
	value := params["value"].(int)
	println("执行任务:", name, "值:", value)
	return nil
})
```

#### 5. 注册任务

##### 一次性任务

```go
// 创建一个1秒后执行的一次性任务
onceTask := task.NewOnceTask(
    "task-id",
    time.Now().Add(time.Second*1),
    0, // 超时时间（0表示不超时）
    nil, // 重试策略
    "example-func-1", // 函数ID
    map[string]any{"key": "value"}, // 任务参数
)

// 注册任务到调度器
sch.Register(onceTask)
```

##### 立即执行任务

```go
// 创建一个立即执行的任务
immediateTask := task.NewImmediateTask(
    "immediate-task-id",
    0, // 超时时间
    nil, // 重试策略
    "example-func-1", // 函数ID
    nil, // 任务参数
)

sch.Register(immediateTask)
```

##### Cron任务

```go
// 创建一个每秒执行一次的Cron任务
cronTask := task.NewCronTask(
    "cron-task-id",
    "*/1 * * * * *", // 秒级Cron表达式
    time.Second*1, // 超时时间
    &task.RetryPolicy{MaxRetry: 3, RetryDelay: time.Second}, // 重试策略
    "example-func-1", // 函数ID
    nil, // 任务参数
)

sch.Register(cronTask)
```

## 高级特性

### 任务超时

任务可以设置超时时间，超过指定时间后会自动取消。

```go
// 创建一个1秒后执行，超时时间为500毫秒的任务
onceTask := task.NewOnceTask(
    "task-id",
    time.Now().Add(time.Second*1),
    time.Millisecond*500, // 超时时间
    nil,
    "example-func-1", // 函数ID
    nil,
)
```

### 任务重试

任务可以设置重试策略，执行失败后会自动重试。

```go
// 创建一个带重试策略的任务
retryPolicy := &task.RetryPolicy{
    MaxRetry:   3,           // 最大重试次数
    RetryDelay: time.Second, // 重试间隔
}

cronTask := task.NewCronTask(
    "cron-task-id",
    "*/1 * * * * *",
    0,
    retryPolicy,
    "example-func-1", // 函数ID
    nil,
)
```

### 任务取消

可以通过任务ID取消正在等待执行的任务。

```go
// 注册任务
sch.Register(task)

// 取消任务
sch.Cancel("task-id")
```

### 任务恢复

可以通过任务ID恢复已取消的任务，支持一次性任务和Cron任务。

```go
// 取消任务
sch.Cancel("task-id")

// 恢复任务
sch.Resume("task-id")
```

任务恢复的特点：
- 支持恢复一次性任务和Cron任务
- 智能处理过期任务：
  - 一次性任务：如果执行时间已过，设置为当前时间立即执行
  - Cron任务：如果执行时间已过，重新计算下次执行时间
- 保持任务的原有属性和配置
- 线程安全，支持并发操作

## 运行测试

```bash
go test ./...
```

## 性能特点

- 基于优先级队列的高效调度
- 支持高并发任务执行
- 低延迟的任务触发
- 灵活的任务类型支持
- 轻量级设计，资源占用少

## 应用场景

- 定时任务调度
- 延迟任务处理
- 周期性任务执行
- 批量任务处理
- 异步任务队列
- 后台任务处理


## TODO

- [x] 针对已取消的cron任务,增加恢复功能
- [x] 记录重试后仍失败的任务
- [ ] 支持任务持久化(redis缓存,记录待执行任务和一段时间内完成的 任务)
- [ ] 支持分布式任务调度
- [ ] 支持任务查询(待执行任务,失败任务,已取消任务,已执行任务(一段时间内))
- [ ] 支持任务优先级

