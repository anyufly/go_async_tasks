package broker

import (
	"context"

	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/task"
	"github.com/fakerjeff/go_async_tasks/messages"
)

type IBroker interface {
	Consume(ctx context.Context, queueName string, prefetch int) (consumer.IConsumer, error)
	Enqueue(ctx context.Context, message *messages.Message) error
	RegisterTask(tasks ...task.ITask)
	GetTask(taskName string) (task.ITask, error)
	HasTask(taskName string) bool
	ConsumeDelayDirect() bool
	Close()
}
