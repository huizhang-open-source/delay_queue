package delay_queue

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type Server struct {
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) AddTasks(tasks []Task) *Server {
	for _, task := range tasks {
		if task.AckTimeout <= 0 {
			task.AckTimeout = 3
		}
		script := GetMessageLuaScript[task.AckType]
		if script == "" {
			task.AckType = AckTypeAuto
		}
		task.runningState = &taskRunningState{
			waitingQueueKey:   waitingQueueKey(task.Name),
			doingQueueKey:     doingQueueKey(task.Name),
			luaSha1AckMonitor: sha1Script(AckMonitorScript),
			luaSha1GetMessage: sha1Script(script),
			mutex:             &sync.Mutex{},
		}
		registeredTasks[task.Name] = task
	}
	return s
}

func (s *Server) Start() {
	log.Printf("启动consumer！")
	go s.startConsumer()
	log.Printf("启动ack monitor！")
	go s.startAckMonitor()
}

func (s *Server) startAckMonitor() {
	var wg sync.WaitGroup
	for {
		for _, task := range registeredTasks {
			if task.AckType == AckTypeDisable {
				continue
			}

			wg.Add(1)
			go func(task Task) {
				ctx, cancel := context.WithCancel(context.Background())
				defer func() {
					wg.Done()
					cancel()
					if r := recover(); r != nil {
						log.Printf("任务:%s, Ack monitor 其它异常:%v\n", task.Name, r)
						task.Consumer.Error(ctx, task, &Error{
							Err:        r.(error),
							ErrMessage: "Ack monitor 其它异常",
						})
					}
				}()

				_, err := execLuaScript(ctx, task.Redis, task.runningState.luaSha1AckMonitor, AckMonitorScript, []interface{}{
					3,
					task.runningState.doingQueueKey,
					task.runningState.waitingQueueKey,
					10000,
					time.Now().Unix() - int64(task.AckTimeout),
				})
				if err != nil {
					log.Printf("任务:%s, Ack monitor 异常:%v\n", task.Name, err)
					task.Consumer.Error(ctx, task, &Error{
						Err:        err,
						ErrMessage: "Ack monitor 异常",
					})
				}

			}(task)
		}
		wg.Wait()
		time.Sleep(time.Second * 1)
	}
}

func (s *Server) startConsumer() {
	for {
		for _, task := range registeredTasks {
			waitingStartConsumerNum := task.ConsumerNum - task.runningState.runningConsumerNum
			for i := 0; i < waitingStartConsumerNum; i++ {
				task.runningState.incrRunningConsumerNum()
				go func(task Task) {
					var messages []string
					ctx, cancel := context.WithCancel(context.Background())
					defer func() {
						cancel()
						if r := recover(); r != nil {
							log.Printf("任务:%s, 其它异常:%v\n", task.Name, r)
							task.Consumer.Error(ctx, task, &Error{
								Err:        r.(error),
								ErrMessage: "其它异常",
								Messages:   messages,
							})
						}
						task.runningState.decrRunningConsumerNum()
					}()
					messages = getWaitingMessages(ctx, task)

					dealMessages(ctx, task, messages)
				}(task)
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func dealMessages(ctx context.Context, task Task, messages []string) {
	if len(messages) > 0 {
		err := task.Consumer.Deal(ctx, task, messages)
		if err != nil {
			log.Printf("任务:%s, 数据处理异常:%s", task.Name, err.Error())
			task.Consumer.Error(ctx, task, &Error{
				Err:        err,
				ErrMessage: "数据处理异常",
				Messages:   messages,
			})
			return
		}

		if task.AckType != AckTypeAuto {
			return
		}

		err = task.Ack(ctx, messages...)
		if err != nil {
			log.Printf("任务:%s, Ack异常:%s", task.Name, err.Error())
			task.Consumer.Error(ctx, task, &Error{
				Err:        err,
				ErrMessage: "Ack异常",
				Messages:   messages,
			})
		}
	}
}

func getWaitingMessages(ctx context.Context, task Task) []string {
	res, err := execLuaScript(
		ctx,
		task.Redis,
		task.runningState.luaSha1GetMessage,
		GetMessageLuaScript[task.AckType],
		[]interface{}{
			3,
			task.runningState.waitingQueueKey,
			task.runningState.doingQueueKey,
			task.Limit,
			time.Now().Unix() - int64(task.DelayTime),
			time.Now().Unix(),
		},
	)
	var messages []string
	if err != nil {
		log.Printf("任务:%s, 获取 messages 异常:%s", task.Name, err.Error())
		task.Consumer.Error(ctx, task, &Error{
			Err:        err,
			ErrMessage: "获取 messages 异常",
		})
		return messages
	}

	for _, item := range res.([]interface{}) {
		messages = append(messages, fmt.Sprintf("%s", item))
	}
	return messages
}
