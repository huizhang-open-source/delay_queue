package delay_queue

import (
	"context"
)

type Producer struct {
	redis             Redis
	luaSha1DelMessage string
}

func NewProducer() *Producer {
	return &Producer{
		luaSha1DelMessage: sha1Script(DelMessageScript),
	}
}

func (p *Producer) RegisterRedis(redis Redis) *Producer {
	p.redis = redis
	return p
}

func (p *Producer) PushMessage(ctx context.Context, taskName string, message string) error {
	return p.redis.ZAdd(ctx, waitingQueueKey(taskName), message)
}

func (p *Producer) DeleteMessage(ctx context.Context, taskName string, message string) error {
	_, err := execLuaScript(ctx, p.redis, p.luaSha1DelMessage, DelMessageScript, []interface{}{
		2,
		waitingQueueKey(taskName),
		doingQueueKey(taskName),
		message,
	})
	return err
}
