package delay_queue

import (
	"context"
	"fmt"
	"sync"
)

type (
	Task struct {
		Name         string            // 任务名称
		DelayTime    int               // 延迟时间
		Limit        int               // 消费者单次消费的最大数据量
		Consumer     Consumer          // 消费者
		ConsumerNum  int               // 消费者数量
		Redis        Redis             // redis客户端
		AckType      string            // ack 类型 0:自动ack 1:手动ack 2:无需ack
		AckTimeout   int               // ack的超时时间(s),默认3s
		runningState *taskRunningState // 任务运行时状态
	}
	taskRunningState struct {
		runningConsumerNum int         // 正在运行的消费者数量
		doingQueueKey      string      // doing队列key
		waitingQueueKey    string      // waiting队列key
		luaSha1AckMonitor  string      // ack lua脚本sha1标识
		luaSha1GetMessage  string      // get messages lua脚本sha1标识
		mutex              *sync.Mutex // 此结构体互斥锁
	}
)

var (
	AckTypeAuto     = "auto"
	AckTypeManual   = "manual"
	AckTypeDisable  = "disable"
	registeredTasks = map[string]Task{}
)

func (t *Task) Ack(ctx context.Context, messages ...string) error {
	return t.Redis.ZRem(ctx, t.runningState.doingQueueKey, messages...)
}

func (t *taskRunningState) incrRunningConsumerNum() {
	t.mutex.Lock()
	t.runningConsumerNum++
	t.mutex.Unlock()
}

func (t *taskRunningState) decrRunningConsumerNum() {
	t.mutex.Lock()
	t.runningConsumerNum--
	t.mutex.Unlock()
}

func waitingQueueKey(taskName string) string {
	return fmt.Sprintf("{%s}:waiting", taskName)
}

func doingQueueKey(taskName string) string {
	return fmt.Sprintf("{%s}:doing", taskName)
}
