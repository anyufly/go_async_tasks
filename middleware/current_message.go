package middleware

import (
	"context"
	"sync"

	"github.com/fakerjeff/go_async_tasks/consts"
	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/middleware"
	"github.com/fakerjeff/go_async_tasks/iface/task"
)

var messageMap *sync.Map
var once sync.Once

func init() {
	once.Do(func() {
		messageMap = new(sync.Map)
	})
}

func GetCurrentMessage(workerId string) consumer.IConsumerMessage {
	if obj, ok := messageMap.Load(workerId); ok {
		return obj.(consumer.IConsumerMessage)
	}
	return nil
}

type CurrentMessage struct {
}

func NewCurrentMessage() *CurrentMessage {
	return &CurrentMessage{}
}

func (c *CurrentMessage) Phase() []int {
	return []int{middleware.AfterProcessMessage, middleware.BeforeProcessMessage}
}

func (c *CurrentMessage) Name() string {
	return consts.CurrentMessageMiddleware
}

func (c *CurrentMessage) BeforeProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error {
	messageMap.Store(message.WorkerID(), message)
	return nil
}

func (c *CurrentMessage) AfterProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage, result []*task.Result) error {
	messageMap.Delete(message.WorkerID())
	message.ClearWorker()
	return nil
}
