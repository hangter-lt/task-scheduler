package task

import "context"

// HandleFunc 任务执行函数类型
type HandleFunc func(ctx context.Context, params map[string]any) error

// FuncID 函数标识符类型
type FuncID string

// FuncRegistry 函数注册表，存储函数ID到实际函数的映射
var FuncRegistry = make(map[FuncID]HandleFunc)

// RegisterFunc 注册任务执行函数
func RegisterFunc(id FuncID, f HandleFunc) {
	FuncRegistry[id] = f
}

// GetFunc 根据ID获取任务执行函数
func GetFunc(id FuncID) (HandleFunc, bool) {
	f, ok := FuncRegistry[id]
	return f, ok
}
