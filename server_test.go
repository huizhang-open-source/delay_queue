package delay_queue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestServerStart(t *testing.T) {
	NewServer().AddTasks([]Task{
		{
			Name:        "delay-queue-1",
			DelayTime:   10,
			Limit:       50,
			Consumer:    DelayQueue1Consumer{},
			ConsumerNum: 1,
			Redis:       defaultRedis(),
			AckType:     AckTypeAuto,
			AckTimeout:  30,
		},
		{
			Name:        "delay-queue-2",
			DelayTime:   10,
			Limit:       2,
			Consumer:    DelayQueue2Consumer{},
			ConsumerNum: 3,
			Redis:       defaultRedis(),
			AckType:     AckTypeAuto,
			AckTimeout:  3,
		},
	}).Start()
	time.Sleep(10000 * time.Second)
}

func TestAckTypeManual(t *testing.T) {
	NewServer().AddTasks([]Task{
		{
			Name:        "delay-queue-3",
			DelayTime:   10,
			Limit:       2,
			Consumer:    DelayQueue3Consumer{},
			ConsumerNum: 3,
			Redis:       defaultRedis(),
			AckType:     AckTypeManual,
			AckTimeout:  30,
		},
	}).Start()
	time.Sleep(10000 * time.Second)
}

func TestAckTypeDisable(t *testing.T) {
	NewServer().AddTasks([]Task{
		{
			Name:        "delay-queue-4",
			DelayTime:   10,
			Limit:       2,
			Consumer:    DelayQueue4Consumer{},
			ConsumerNum: 3,
			Redis:       defaultRedis(),
			AckType:     AckTypeDisable,
			AckTimeout:  30,
		},
	}).Start()
	time.Sleep(10000 * time.Second)
}

type DelayQueue1Consumer struct {
}

func (d DelayQueue1Consumer) Deal(ctx context.Context, task Task, messages []string) error {
	fmt.Println("DelayQueue1Consumer Deal", messages)
	return nil
}

func (d DelayQueue1Consumer) Error(ctx context.Context, task Task, err *Error) {
	fmt.Println("DelayQueue1Consumer Error", *err)
}

type DelayQueue2Consumer struct {
}

func (d DelayQueue2Consumer) Deal(ctx context.Context, task Task, messages []string) error {
	fmt.Println("DelayQueue2Consumer Deal", messages)
	return nil
}

func (d DelayQueue2Consumer) Error(ctx context.Context, task Task, err *Error) {
	fmt.Println("DelayQueue2Consumer Error", *err)
}

type DelayQueue3Consumer struct {
}

func (d DelayQueue3Consumer) Deal(ctx context.Context, task Task, messages []string) error {
	fmt.Println("DelayQueue3Consumer Deal", messages)
	err := task.Ack(ctx, messages...)
	fmt.Println(err)
	return nil
}

func (d DelayQueue3Consumer) Error(ctx context.Context, task Task, err *Error) {
	fmt.Println("DelayQueue3Consumer Error", *err)
}

type DelayQueue4Consumer struct {
}

func (d DelayQueue4Consumer) Deal(ctx context.Context, task Task, messages []string) error {
	fmt.Println("DelayQueue4Consumer Deal", messages)
	return nil
}

func (d DelayQueue4Consumer) Error(ctx context.Context, task Task, err *Error) {
	fmt.Println("DelayQueue4Consumer Error", *err)
}

func defaultRedis() DefaultRedis {
	return DefaultRedis{
		Host: "0.0.0.0",
		Port: 6379,
	}
}
