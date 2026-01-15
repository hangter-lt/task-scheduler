package task

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// 周期性任务
type CronTask struct {
	BaseTask
	cronExpr   string      // cron表达式
	cronParser cron.Parser // cron解析器
}

func NewCronTask(
	id string,
	cronExpr string,
	timeout time.Duration,
	retryPolicy *RetryPolicy,
	funcID FuncID,
	params any,
) *CronTask {
	if retryPolicy == nil {
		retryPolicy = &RetryPolicy{
			MaxRetry:   0,
			RetryDelay: 0,
		}
	}

	// cron表达式解析
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(cronExpr)
	if err != nil {
		panic(fmt.Errorf("failed to parse cron expression %s: %w", cronExpr, err))
	}
	nextExec := schedule.Next(time.Now())

	return &CronTask{
		BaseTask: BaseTask{
			id:           id,
			timeout:      timeout,
			retryPolicy:  retryPolicy,
			nextExecTime: nextExec,
			taskType:     TaskTypeCron,
			funcID:       funcID,
			params:       params,
		},
		cronExpr:   cronExpr,
		cronParser: parser,
	}
}

func (c *CronTask) UpdateNextExecTime() {
	schedule, err := c.cronParser.Parse(c.cronExpr)
	if err != nil {
		// 记录日志
		fmt.Printf("Failed to parse cron expression %s: %v\n", c.cronExpr, err)
		return
	}
	nextExec := schedule.Next(time.Now())
	c.SetNextExecTime(nextExec)
}

func (c *CronTask) CronExpr() string {
	return c.cronExpr
}
