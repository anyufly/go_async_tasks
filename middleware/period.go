package middleware

import (
	"context"

	"github.com/fakerjeff/go_async_tasks/consts"
	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/middleware"
	"github.com/fakerjeff/go_async_tasks/iface/task"
)

type Period struct {
}

func (p Period) Phase() []int {
	return []int{
		middleware.AfterProcessMessage,
	}
}

func (p Period) Name() string {
	return consts.PeriodMiddleware
}

func (p Period) AfterProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage, result []*task.Result) error {
	panic("implement me")
}
