package delay_queue

import (
	"context"
)

type Consumer interface {
	Deal(ctx context.Context, task Task, messages []string) error
	Error(ctx context.Context, task Task, err *Error)
}

type Error struct {
	Err        error
	ErrMessage string
	Messages   []string
}
