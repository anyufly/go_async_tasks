package middleware

import (
	"context"

	"github.com/fakerjeff/go_async_tasks/config"
	"github.com/fakerjeff/go_async_tasks/consts"

	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/middleware"
	"github.com/fakerjeff/go_async_tasks/iface/task"
)

type Group struct {
}

func NewGroup() *Group {
	return &Group{}
}

func (g *Group) Phase() []int {
	return []int{
		middleware.AfterProcessMessage,
	}
}

func (g *Group) Name() string {
	return consts.GroupMiddleware
}

func (g *Group) AfterProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage, result []*task.Result) error {
	messageErr := message.Err()
	if messageErr != nil {
		return messageErr
	}
	m := message.Message()
	groupId := m.Options.GroupID
	callbacks := m.Options.GroupCallbacks
	if groupId != "" {
		b := config.Barrier().Load(groupId)
		wait, err := b.Wait()
		if err != nil {
			return err
		}
		if !wait {
			if len(callbacks) > 0 {
				for _, msg := range callbacks {
					e := broker.Enqueue(ctx, msg)
					if e != nil {
						return e
					}
				}
			}
			_ = config.Barrier().Delete(groupId)
			if holder, ok := middleware.Hodler(broker); ok && holder.HasMiddleware(consts.ResultsMiddleware) {
				_ = config.Backend().DeleteGroup(ctx, groupId)
			}
		}
	}

	return nil
}
