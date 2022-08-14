package delay_queue

import (
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"time"
)

type Redis interface {
	ZAdd(ctx context.Context, key string, messages ...string) error
	ZRem(ctx context.Context, key string, messages ...string) error
	EvalSha(ctx context.Context, sha1 string, values []interface{}) (interface{}, error)
	LoadScript(ctx context.Context, script string) error
}

type DefaultRedis struct {
	Host string
	Port int
}

func (d DefaultRedis) ZAdd(ctx context.Context, key string, messages ...string) error {
	args := []interface{}{
		key,
		"NX",
		time.Now().Unix(),
	}
	for _, message := range messages {
		args = append(args, message)
	}
	_, err := d.execCommand("ZADD", args...)
	return err
}

func (d DefaultRedis) ZRem(ctx context.Context, key string, messages ...string) error {
	args := []interface{}{
		key,
	}
	for _, message := range messages {
		args = append(args, message)
	}
	_, err := d.execCommand("ZREM", args...)
	return err
}

func (d DefaultRedis) EvalSha(ctx context.Context, sha1 string, values []interface{}) (interface{}, error) {
	args := []interface{}{
		sha1,
	}
	args = append(args, values...)
	res, err := d.execCommand("EVALSHA", args...)
	return res, err
}

func (d DefaultRedis) LoadScript(ctx context.Context, script string) error {
	args := []interface{}{
		"LOAD",
		script,
	}
	_, err := d.execCommand("SCRIPT", args...)
	return err
}

func (d DefaultRedis) execCommand(command string, args ...interface{}) (interface{}, error) {
	conn, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", d.Host, d.Port))
	if err != nil {
		panic(err.Error())
	}
	return conn.Do(command, args...)
}
