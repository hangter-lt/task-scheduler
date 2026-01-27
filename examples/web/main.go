package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/hangter-lt/task-scheduler/executor"
	"github.com/hangter-lt/task-scheduler/persistence"
	"github.com/hangter-lt/task-scheduler/scheduler"
	"github.com/hangter-lt/task-scheduler/task"
)

var (
	exec             *executor.Executor
	sch              *scheduler.Scheduler
	redisPersistence *persistence.RedisPersistence
)

func init() {
	var err error
	// 创建执行器
	exec, err = executor.NewExecutor(10)
	if err != nil {
		log.Fatalf("创建执行器失败: %v", err)
	}

	// 检查是否有Redis配置
	// redisAddr := "localhost:6379"
	redisAddr := ""
	if redisAddr != "" {
		redisPersistence = persistence.NewRedisPersistence(redisAddr, "", 2)
		sch = scheduler.NewSchedulerWithPersistence(exec, redisPersistence, "web-server")
	} else {
		sch = scheduler.NewScheduler(exec)
	}

	// 启动调度器
	fmt.Println("启动调度器...")
	go sch.Run()
	fmt.Println("调度器启动成功")

	// 注册一些示例函数
	task.RegisterFunc("example-func", func(ctx context.Context, params any) error {
		fmt.Printf("执行示例任务，参数: %v\n", params)
		fmt.Printf("time.Now(): %v\n", time.Now())
		time.Sleep(3 * time.Second)
		return nil
	})

	task.RegisterFunc("error-func", func(ctx context.Context, params any) error {
		fmt.Printf("执行错误示例任务，参数: %v\n", params)
		time.Sleep(1 * time.Second)
		return fmt.Errorf("模拟任务执行失败")
	})
}

func main() {
	// 注册API路由
	http.HandleFunc("/api/tasks", handleTasks)
	http.HandleFunc("/api/tasks/", handleTaskOperations)
	http.HandleFunc("/api/failures", handleFailures)
	http.HandleFunc("/api/failures/", handleFailureDetail)
	http.HandleFunc("/api/funcs", handleFuncs)
	http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "服务器正常响应")
	})

	// 注册静态文件路由
	http.Handle("/", http.FileServer(http.Dir("./web/static")))

	// 启动服务器
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Web服务器启动在 http://localhost:%s\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// 通用响应结构
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// 任务响应结构
type TaskResponse struct {
	ID           string      `json:"id"`
	Type         string      `json:"type"`
	Status       string      `json:"status"`
	NextExecTime time.Time   `json:"nextExecTime"`
	FuncID       string      `json:"funcId"`
	Params       interface{} `json:"params"`
	CronExpr     string      `json:"cronExpr,omitempty"`
}

// 转换任务为响应结构
func taskToResponse(t task.Task) TaskResponse {
	return TaskResponse{
		ID:           t.ID(),
		Type:         string(t.Type()),
		Status:       string(t.Status()),
		NextExecTime: t.NextExecTime(),
		FuncID:       string(t.FuncID()),
		Params:       t.Params(),
		CronExpr:     t.CronExpr(),
	}
}

// 任务创建请求结构
type CreateTaskRequest struct {
	ID         string      `json:"id"`
	Type       string      `json:"type"`
	ExecTime   string      `json:"execTime,omitempty"`
	CronExpr   string      `json:"cronExpr,omitempty"`
	Timeout    int64       `json:"timeout,omitempty"`
	MaxRetry   int         `json:"maxRetry,omitempty"`
	RetryDelay int64       `json:"retryDelay,omitempty"`
	FuncID     string      `json:"funcId"`
	Params     interface{} `json:"params"`
}

// 处理任务列表请求
func handleTasks(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("收到请求: %s %s\n", r.Method, r.URL.Path)
	setupCORS(&w, r)

	switch r.Method {
	case http.MethodGet:
		// 获取所有任务
		fmt.Println("处理GET请求")
		tasks := sch.GetAllTasks()
		fmt.Printf("获取到 %d 个任务\n", len(tasks))

		// 转换任务为响应结构
		taskResponses := make([]TaskResponse, 0, len(tasks))
		for _, t := range tasks {
			taskResponses = append(taskResponses, taskToResponse(t))
		}

		respondWithJSON(w, http.StatusOK, Response{
			Code:    0,
			Message: "成功",
			Data:    taskResponses,
		})
	case http.MethodPost:
		// 创建新任务
		fmt.Println("处理POST请求")
		var req CreateTaskRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fmt.Printf("解析请求失败: %v\n", err)
			respondWithJSON(w, http.StatusBadRequest, Response{
				Code:    400,
				Message: "无效的请求参数",
			})
			return
		}

		fmt.Printf("req: %v\n", req)

		var newTask task.Task

		// 解析超时时间
		timeout := time.Duration(req.Timeout) * time.Millisecond

		// 解析重试策略
		retryPolicy := &task.RetryPolicy{
			MaxRetry:     req.MaxRetry,
			RetryDelay:   time.Duration(req.RetryDelay) * time.Millisecond,
			CurrentRetry: 0,
		}

		switch req.Type {
		case "once":
			// 解析执行时间
			fmt.Println("处理一次性任务")
			execTime, err := time.Parse(time.RFC3339, req.ExecTime)
			if err != nil {
				fmt.Printf("解析执行时间失败: %v\n", err)
				respondWithJSON(w, http.StatusBadRequest, Response{
					Code:    400,
					Message: "无效的执行时间格式",
				})
				return
			}
			newTask = task.NewOnceTask(req.ID, execTime, timeout, retryPolicy, task.FuncID(req.FuncID), req.Params)
		case "immediate":
			// 将params转换为map[string]any
			fmt.Println("处理立即执行任务")
			paramsMap, ok := req.Params.(map[string]any)
			if !ok {
				fmt.Println("参数格式无效")
				respondWithJSON(w, http.StatusBadRequest, Response{
					Code:    400,
					Message: "无效的参数格式",
				})
				return
			}
			fmt.Printf("转换后的参数: %+v\n", paramsMap)
			newTask = task.NewImmediateTask(req.ID, timeout, retryPolicy, task.FuncID(req.FuncID), paramsMap)
		case "cron":
			fmt.Println("处理Cron任务")
			if req.CronExpr == "" {
				fmt.Println("Cron表达式为空")
				respondWithJSON(w, http.StatusBadRequest, Response{
					Code:    400,
					Message: "Cron表达式不能为空",
				})
				return
			}
			newTask = task.NewCronTask(req.ID, req.CronExpr, timeout, retryPolicy, task.FuncID(req.FuncID), req.Params)
		default:
			fmt.Printf("无效的任务类型: %s\n", req.Type)
			respondWithJSON(w, http.StatusBadRequest, Response{
				Code:    400,
				Message: "无效的任务类型",
			})
			return
		}

		fmt.Printf("newTask: %v\n", newTask)
		// 注册任务
		fmt.Printf("注册任务: %+v\n", newTask)
		sch.Register(newTask)
		fmt.Println("任务注册成功")
		respondWithJSON(w, http.StatusCreated, Response{
			Code:    0,
			Message: "任务创建成功",
			Data:    taskToResponse(newTask),
		})
	default:
		fmt.Printf("不支持的方法: %s\n", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// 处理任务操作请求（详情、取消、恢复、重试）
func handleTaskOperations(w http.ResponseWriter, r *http.Request) {
	setupCORS(&w, r)

	// 提取任务ID和操作
	path := r.URL.Path[len("/api/tasks/"):]
	if path == "" {
		respondWithJSON(w, http.StatusBadRequest, Response{
			Code:    400,
			Message: "任务ID不能为空",
		})
		return
	}

	// 检查是否有操作后缀
	if len(path) > len("/cancel") && path[len(path)-len("/cancel"):] == "/cancel" {
		// 取消任务
		taskID := path[:len(path)-len("/cancel")]
		sch.Cancel(taskID)
		respondWithJSON(w, http.StatusOK, Response{
			Code:    0,
			Message: "任务取消成功",
		})
		return
	} else if len(path) > len("/resume") && path[len(path)-len("/resume"):] == "/resume" {
		// 恢复任务
		taskID := path[:len(path)-len("/resume")]
		sch.Resume(taskID)
		respondWithJSON(w, http.StatusOK, Response{
			Code:    0,
			Message: "任务恢复成功",
		})
		return
	} else if len(path) > len("/retry") && path[len(path)-len("/retry"):] == "/retry" {
		// 重试任务
		taskID := path[:len(path)-len("/retry")]
		// 从查询参数中获取recordId
		recordID := r.URL.Query().Get("recordId")
		err := sch.RetryFailedTask(taskID, recordID)
		if err != nil {
			respondWithJSON(w, http.StatusInternalServerError, Response{
				Code:    500,
				Message: fmt.Sprintf("任务重试失败: %v", err),
			})
			return
		}
		respondWithJSON(w, http.StatusOK, Response{
			Code:    0,
			Message: "任务重试成功",
		})
		return
	} else if len(path) > len("/execute") && path[len(path)-len("/execute"):] == "/execute" {
		// 立即执行任务
		taskID := path[:len(path)-len("/execute")]
		err := sch.ExecuteTaskImmediately(taskID)
		if err != nil {
			respondWithJSON(w, http.StatusInternalServerError, Response{
				Code:    500,
				Message: fmt.Sprintf("任务立即执行失败: %v", err),
			})
			return
		}
		respondWithJSON(w, http.StatusOK, Response{
			Code:    0,
			Message: "任务立即执行成功",
		})
		return
	} else {
		// 获取任务详情
		taskID := path
		t := sch.GetTask(taskID)
		if t == nil {
			respondWithJSON(w, http.StatusNotFound, Response{
				Code:    404,
				Message: "任务不存在",
			})
			return
		}
		respondWithJSON(w, http.StatusOK, Response{
			Code:    0,
			Message: "成功",
			Data:    t,
		})
		return
	}
}

// 处理失败记录请求
func handleFailures(w http.ResponseWriter, r *http.Request) {
	setupCORS(&w, r)

	if r.Method == http.MethodGet {
		// 获取所有失败记录
		records := sch.GetAllFailureRecords()
		respondWithJSON(w, http.StatusOK, Response{
			Code:    0,
			Message: "成功",
			Data:    records,
		})
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// 处理失败记录详情请求
func handleFailureDetail(w http.ResponseWriter, r *http.Request) {
	setupCORS(&w, r)

	// 提取任务ID
	taskID := r.URL.Path[len("/api/failures/"):]
	if taskID == "" {
		respondWithJSON(w, http.StatusBadRequest, Response{
			Code:    400,
			Message: "任务ID不能为空",
		})
		return
	}

	// 获取任务的失败记录
	records := sch.GetFailureRecords(taskID)
	respondWithJSON(w, http.StatusOK, Response{
		Code:    0,
		Message: "成功",
		Data:    records,
	})
}

// 处理函数列表请求
func handleFuncs(w http.ResponseWriter, r *http.Request) {
	setupCORS(&w, r)

	if r.Method == http.MethodGet {
		// 获取所有注册的函数
		funcs := task.GetAllFuncIDs()
		respondWithJSON(w, http.StatusOK, Response{
			Code:    0,
			Message: "成功",
			Data:    funcs,
		})
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// 响应JSON格式数据
func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

// 设置CORS头
func setupCORS(w *http.ResponseWriter, r *http.Request) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
	(*w).Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	(*w).Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
}
