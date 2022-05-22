package middleware

import (
	"context"
	"reflect"

	"github.com/fakerjeff/go_async_tasks/consts"

	"github.com/fakerjeff/go_async_tasks/config"

	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/middleware"
	"github.com/fakerjeff/go_async_tasks/iface/task"
)

type Results struct {
}

func NewResults() *Results {
	return &Results{}
}

func (r Results) Phase() []int {
	return []int{
		middleware.AfterProcessMessage,
		middleware.AfterNack,
	}
}

func (r Results) Name() string {
	return consts.ResultsMiddleware
}

func (r Results) AfterNack(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error {
	m := message.Message()
	if m.Options.MaxRetries != 0 && m.Options.Retries < m.Options.MaxRetries {
		return nil
	}
	messageErr := message.Err()
	result := []*task.Result{
		{
			Type:  "string",
			Value: reflect.TypeOf(messageErr).String(),
		},
		{
			Type:  "string",
			Value: messageErr.Error(),
		},
	}
	if m.Options.GroupID != "" {
		err := config.Backend().GroupCompleteOne(ctx, m.Options.GroupID, m.ID, result)
		if err != nil {
			return err
		}

	} else {
		err := config.Backend().Save(ctx, m.ID, result)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r Results) AfterProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage, result []*task.Result) error {
	if message.Err() == nil {
		m := message.Message()
		if m.Options.GroupID != "" {
			err := config.Backend().GroupCompleteOne(ctx, m.Options.GroupID, m.ID, result)
			if err != nil {
				return err
			}

		} else {
			err := config.Backend().Save(ctx, m.ID, result)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
