package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hangter-lt/task-scheduler/task"
)

// RedisPersistence Redis持久化实现
type RedisPersistence struct {
	client *redis.Client
	ctx    context.Context
}

// NewRedisPersistence 创建新的Redis持久化实例
func NewRedisPersistence(addr string, password string, db int) *RedisPersistence {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &RedisPersistence{
		client: client,
		ctx:    context.Background(),
	}
}

// AcquireLock 获取分布式锁
// taskID: 任务ID
// expire: 锁过期时间
// 返回值: 是否获取成功
func (r *RedisPersistence) AcquireLock(taskID string, expire time.Duration, nodeFlag string) (bool, error) {
	key := "lock:" + taskID

	// 使用SETNX命令获取锁
	return r.client.SetNX(r.ctx, key, nodeFlag, expire).Result()
}

// ReleaseLock 释放分布式锁
// taskID: 任务ID
// 使用Lua脚本确保解锁的原子性
func (r *RedisPersistence) ReleaseLock(taskID string, nodeFlag string) error {
	key := "lock:" + taskID

	// Lua脚本：只有当锁值匹配当前节点ID时才删除锁
	luaScript := `
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
	`

	// 执行Lua脚本
	_, err := r.client.Eval(r.ctx, luaScript, []string{key}, nodeFlag).Result()
	return err
}

// SaveTask 保存任务到Redis
func (r *RedisPersistence) SaveTask(t task.Task) error {
	// 创建任务数据映射
	taskData := make(map[string]interface{})

	// 保存基本任务信息
	taskData["id"] = t.ID()
	taskData["taskType"] = t.Type()
	taskData["status"] = t.Status()
	taskData["nextExecTime"] = t.NextExecTime()
	taskData["timeout"] = t.Timeout()
	taskData["retryPolicy"] = t.RetryPolicy()
	taskData["funcID"] = t.FuncID()
	taskData["params"] = t.Params()

	// 根据任务类型保存额外字段
	if t.Type() == task.TaskTypeCron {
		taskData["cronExpr"] = t.CronExpr()
	}

	// 序列化任务数据
	data, err := json.Marshal(taskData)
	if err != nil {
		return err
	}

	// 保存到Redis
	return r.client.Set(r.ctx, "task:"+t.ID(), data, 0).Err()
}

// LoadTask 从Redis加载任务
func (r *RedisPersistence) LoadTask(id string) (task.Task, error) {
	// 从Redis获取任务数据
	data, err := r.client.Get(r.ctx, "task:"+id).Bytes()
	if err != nil {
		return nil, err
	}

	// 反序列化任务数据
	var taskData map[string]any
	if err := json.Unmarshal(data, &taskData); err != nil {
		return nil, err
	}

	// 根据任务类型重建任务对象
	taskType := task.TaskType(taskData["taskType"].(string))
	var retryPolicy task.RetryPolicy
	retryPolicyB, err := json.Marshal(taskData["retryPolicy"])
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(retryPolicyB, &retryPolicy); err != nil {
		return nil, err
	}

	// 获取任务状态，如果不存在则设置默认值
	status := task.TaskStatusPending
	if statusVal, ok := taskData["status"]; ok {
		if statusStr, ok := statusVal.(string); ok {
			status = task.TaskStatus(statusStr)
		}
	}

	var t task.Task

	switch taskType {
	case task.TaskTypeOnce:
		t = task.NewOnceTask(
			taskData["id"].(string),
			taskData["nextExecTime"].(int64),
			time.Duration(int(taskData["timeout"].(float64))),
			&retryPolicy,
			task.FuncID(taskData["funcID"].(string)),
			taskData["params"].(map[string]any),
		)
	case task.TaskTypeCron:
		t = task.NewCronTask(
			taskData["id"].(string),
			taskData["cronExpr"].(string),
			time.Duration(int(taskData["timeout"].(float64))),
			&retryPolicy,
			task.FuncID(taskData["funcID"].(string)),
			taskData["params"].(map[string]any),
		)
	default:
		return nil, fmt.Errorf("unknown task type: %s", taskType)
	}

	// 设置任务状态
	t.SetStatus(status)

	return t, nil
}

// LoadAllTasks 加载所有任务
func (r *RedisPersistence) LoadAllTasks() ([]task.Task, error) {
	// 查找所有任务键
	keys, err := r.client.Keys(r.ctx, "task:*").Result()
	if err != nil {
		return nil, err
	}

	// 加载所有任务
	tasks := make([]task.Task, 0, len(keys))
	for _, key := range keys {
		id := key[5:] // 移除"task:"前缀
		t, err := r.LoadTask(id)
		if err != nil {
			continue
		}
		tasks = append(tasks, t)
	}

	return tasks, nil
}

// DeleteTask 从Redis删除任务
func (r *RedisPersistence) DeleteTask(id string) error {
	return r.client.Del(r.ctx, "task:"+id).Err()
}

// Close 关闭Redis连接
func (r *RedisPersistence) Close() error {
	return r.client.Close()
}

// SaveFailureRecord 保存任务失败记录到Redis
func (r *RedisPersistence) SaveFailureRecord(record task.FailureRecord) error {
	// 序列化失败记录
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// 保存到Redis列表，使用taskId作为key的一部分
	key := "failure:" + record.TaskID
	return r.client.RPush(r.ctx, key, data).Err()
}

// LoadFailureRecords 加载任务的失败记录
func (r *RedisPersistence) LoadFailureRecords(taskID string) ([]task.FailureRecord, error) {
	key := "failure:" + taskID

	// 从Redis获取所有失败记录
	dataList, err := r.client.LRange(r.ctx, key, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	// 反序列化失败记录
	records := make([]task.FailureRecord, 0, len(dataList))
	for _, data := range dataList {
		var record task.FailureRecord
		if err := json.Unmarshal([]byte(data), &record); err != nil {
			continue
		}
		records = append(records, record)
	}

	return records, nil
}

// LoadAllFailureRecords 加载所有任务的失败记录
func (r *RedisPersistence) LoadAllFailureRecords() (map[string][]task.FailureRecord, error) {
	// 查找所有失败记录键
	keys, err := r.client.Keys(r.ctx, "failure:*").Result()
	if err != nil {
		return nil, err
	}

	// 加载所有失败记录
	allRecords := make(map[string][]task.FailureRecord)
	for _, key := range keys {
		taskID := key[8:] // 移除"failure:"前缀
		records, err := r.LoadFailureRecords(taskID)
		if err != nil {
			continue
		}
		allRecords[taskID] = records
	}

	return allRecords, nil
}

// DeleteFailureRecord 删除指定的失败记录
func (r *RedisPersistence) DeleteFailureRecord(taskID string, recordID string) error {
	key := "failure:" + taskID

	// 获取所有失败记录
	records, err := r.LoadFailureRecords(taskID)
	if err != nil {
		return err
	}

	// 过滤掉要删除的记录
	var remainingRecords []task.FailureRecord
	for _, record := range records {
		if record.ID != recordID {
			remainingRecords = append(remainingRecords, record)
		}
	}

	// 重新保存过滤后的记录
	pipe := r.client.Pipeline()

	// 删除原有的所有记录
	pipe.Del(r.ctx, key)

	// 添加过滤后的记录
	for _, record := range remainingRecords {
		data, err := json.Marshal(record)
		if err != nil {
			continue
		}
		pipe.RPush(r.ctx, key, data)
	}

	// 执行管道命令
	_, err = pipe.Exec(r.ctx)
	return err
}

// DeleteAllFailureRecords 删除指定任务的所有失败记录
func (r *RedisPersistence) DeleteAllFailureRecords(taskID string) error {
	key := "failure:" + taskID
	return r.client.Del(r.ctx, key).Err()
}
