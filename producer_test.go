package delay_queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPushMessage(t *testing.T) {
	ctx := context.TODO()
	var wg sync.WaitGroup
	for {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				err := NewProducer().RegisterRedis(defaultRedis()).PushMessage(ctx, "delay-queue-1", fmt.Sprintf("测试数据:%d=%d", time.Now().UnixNano(), i))
				if err != nil {
					t.Error("PushMessage Fail!")
				}
			}(i)
		}
		wg.Wait()
		fmt.Println("next")
		time.Sleep(100 * time.Millisecond)
	}
}

func TestZRem(t *testing.T) {
	ctx := context.TODO()
	message := "测试数据"
	err := NewProducer().RegisterRedis(defaultRedis()).PushMessage(ctx, "delay-queue-1", message)
	if err != nil {
		t.Error("PushMessage Fail!")
	}
	err = NewProducer().RegisterRedis(defaultRedis()).DeleteMessage(ctx, "delay-queue-1", message)
	if err != nil {
		t.Error("DeleteMessage Fail!")
	}
}
