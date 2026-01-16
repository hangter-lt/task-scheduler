# task-scheduler

一个轻量级、高性能的Go语言任务调度系统，支持多种类型的任务调度，包括一次性任务、立即执行任务和Cron表达式任务。

## 项目特点

- 基于优先级队列的高效调度算法
- 支持多种任务类型（一次性、立即执行、Cron）
- 支持任务超时和自动重试
- 高并发设计，支持大量任务调度
- 支持Redis持久化
- 支持分布式任务调度，确保任务执行唯一性
- 易于集成到现有项目中

## 项目结构

```
├── executor/       # 执行器，负责执行任务
│   ├── executor.go
│   └── executor_test.go
├── examples/       # 示例代码
│   ├── multi/          # 分布式示例
│   ├── simple/         # 基本示例
│   └── simple_redis/   # Redis持久化示例
├── persistence/    # 持久化层，支持Redis
│   └── redis.go
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
- 支持分布式任务调度，确保任务执行唯一性
- 任务状态管理（Pending、Running、Completed、Canceled、Failed）

#### API
- `NewScheduler(exec *executor.Executor) *Scheduler` - 创建一个新的调度器
- `NewSchedulerWithPersistence(exec *executor.Executor, persistence *persistence.RedisPersistence, nodeFlag string) *Scheduler` - 创建带有Redis持久化的调度器
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

### 5. 持久化层（Persistence）

持久化层负责将任务状态持久化到Redis，支持分布式任务调度和任务状态的持久化。

#### 主要功能
- 支持任务的保存、加载和删除
- 提供分布式锁机制，确保任务执行唯一性
- 支持任务状态的持久化
- 支持多节点环境下的任务协调

#### API
- `NewRedisPersistence(addr string, password string, db int) *RedisPersistence` - 创建Redis持久化实例
- `SaveTask(t task.Task) error` - 保存任务到Redis
- `LoadTask(id string) (task.Task, error)` - 从Redis加载任务
- `LoadAllTasks() ([]task.Task, error)` - 加载所有任务
- `DeleteTask(id string) error` - 从Redis删除任务
- `AcquireLock(taskID string, expire time.Duration, nodeFlag string) (bool, error)` - 获取分布式锁
- `ReleaseLock(taskID string, nodeFlag string) error` - 释放分布式锁


## 快速开始

### 安装

```bash
go get -u github.com/hangter-lt/task-scheduler
```

### 运行示例

#### 基本示例
```bash
go run examples/simple/main.go
```

#### Redis持久化示例
```bash
go run examples/simple_redis/main.go
```

#### 分布式调度示例
```bash
go run examples/multi/main.go
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

### 任务取消与恢复

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

### Redis持久化

通过Redis持久化，可以在服务重启后恢复任务状态，支持分布式部署。

```go
import (
    "github.com/hangter-lt/task-scheduler/persistence"
)

// 创建Redis持久化层
redisPersistence := persistence.NewRedisPersistence("localhost:6379", "", 0)

// 创建带有Redis持久化的调度器
sch := scheduler.NewSchedulerWithPersistence(exec, redisPersistence, "node-1")
```

### 分布式任务调度

在多节点环境下，通过分布式锁确保任务执行的唯一性。

```go
// 在不同节点上创建调度器，使用不同的nodeFlag
// 节点1
sch1 := scheduler.NewSchedulerWithPersistence(exec1, redisPersistence, "node-1")

// 节点2
sch2 := scheduler.NewSchedulerWithPersistence(exec2, redisPersistence, "node-2")

// 两个节点都会尝试执行任务，但只有一个能获取到锁并执行
// 周期性任务即使抢锁失败，也会重新加入调度队列，不会丢失
```

### 分布式锁机制

分布式锁确保同一任务在同一时间只能被一个节点执行：

- **锁值设计**：包含节点标识和时间戳，确保锁的唯一性
- **过期时间**：默认3分钟，可根据任务超时时间自动调整
- **原子解锁**：使用Lua脚本确保解锁操作的原子性
- **安全解锁**：只有持有锁的节点才能释放锁，避免误删

### 任务状态管理

任务具有以下状态：

- **Pending**：待执行状态，任务等待执行
- **Running**：执行中状态，任务正在执行
- **Completed**：已完成状态，一次性任务执行完成
- **Canceled**：已取消状态，任务被手动取消
- **Failed**：执行失败状态，任务执行失败且重试次数用尽

状态转换流程：
1. 任务创建 → Pending
2. 开始执行 → Running
3. 执行成功 → Completed（一次性任务）/ Pending（周期性任务）
4. 执行失败 → Pending（有重试次数）/ Failed（无重试次数）
5. 手动取消 → Canceled
6. 恢复任务 → Pending

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
- [x] 支持任务持久化(redis缓存,记录待执行任务和一段时间内完成的任务)
- [x] 支持分布式任务调度
- [ ] 支持任务查询(待执行任务,失败任务,已取消任务,已执行任务(一段时间内))
- [ ] 支持任务优先级

