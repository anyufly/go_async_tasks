package middleware

import (
	"context"
	"reflect"

	"github.com/fakerjeff/go_async_tasks/consts"
	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/middleware"
	"github.com/fakerjeff/go_async_tasks/iface/task"
	tasks "github.com/fakerjeff/go_async_tasks/task"
)

type Callbacks struct {
}

func NewCallbacks() *Callbacks {
	return &Callbacks{}
}

func (c *Callbacks) Phase() []int {
	return []int{
		middleware.AfterProcessMessage,
	}
}

func (c *Callbacks) callOrSend(ctx context.Context, t task.ITask, args []task.Arg) error {
	if tt, ok := t.(*tasks.Task); ok {
		tt.WithSendContext(ctx)
		_, eer := tt.Send(args)
		if eer != nil {
			return eer
		}

	} else {
		_, eer := t.Call(args)
		if eer != nil {
			return eer
		}
	}
	return nil
}

func (c *Callbacks) AfterProcessMessage(ctx context.Context, b broker.IBroker, message consumer.IConsumerMessage, result []*task.Result) error {
	m := message.Message()
	t, err := b.GetTask(m.TaskName)
	if err != nil {
		return err
	}
	var successCallbackName, failureCallbackName string
	if tt, ok := t.(*tasks.Task); ok {
		successCallbackName = tt.Options.OnSucess
		failureCallbackName = tt.Options.OnSucess
	} else {
		return nil
	}
	messageErr := message.Err()
	args := m.TaskArgs
	if messageErr == nil {
		if successCallbackName != "" {
			successCallback, ee := b.GetTask(successCallbackName)
			if ee != nil {
				return ee
			}
			for _, r := range result {
				args = append(args, task.Arg{
					Type:  r.Type,
					Value: r.Value,
				})
			}
			return c.callOrSend(ctx, successCallback, args)
		}
	} else {
		if failureCallbackName != "" {
			failureCallback, ee := b.GetTask(successCallbackName)
			if ee != nil {
				return ee
			}

			args = append(args, task.Arg{
				Type:  "string",
				Value: reflect.TypeOf(messageErr).String(),
			}, task.Arg{
				Type:  "string",
				Value: messageErr.Error(),
			})
			return c.callOrSend(ctx, failureCallback, args)
		}
	}
	return nil
}

func (c *Callbacks) Name() string {
	return consts.CallbacksMiddleware
}
