package middleware

import (
	"context"
	"errors"
	"reflect"

	"github.com/fakerjeff/go_async_tasks/util/deepcopy"

	"github.com/fakerjeff/go_async_tasks/consts"
	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/middleware"
	"github.com/fakerjeff/go_async_tasks/iface/task"
	"github.com/fakerjeff/go_async_tasks/messages"
	tasks "github.com/fakerjeff/go_async_tasks/task"
	"github.com/fakerjeff/go_async_tasks/util"
)

var retriesExceeded = errors.New("retries exceeded")

type Retries struct {
}

func NewRetries() *Retries {
	return &Retries{}
}

func (r *Retries) Phase() []int {
	return []int{
		middleware.AfterProcessMessage,
	}
}

func (r *Retries) AfterProcessMessage(ctx context.Context, b broker.IBroker, message consumer.IConsumerMessage, result []*task.Result) error {
	m := message.Message()
	if m.Options.MaxRetries == 0 {
		return nil
	}

	t, err := b.GetTask(m.TaskName)
	if err != nil {
		return err
	}
	var retryWhen tasks.RetryWhen
	var retryEscape tasks.RetryEscape

	if tt, ok := t.(*tasks.Task); ok {
		retryWhen = tt.Options.RetryWhen
		retryEscape = tt.Options.RetryEscape
	}

	messageErr := message.Err()
	if len(retryEscape) > 0 {
		for _, escape := range retryEscape {
			if reflect.TypeOf(messageErr) == reflect.TypeOf(escape) {
				return messageErr
			}
		}
	}
	retries := m.Options.Retries
	if retryWhen != nil && !retryWhen(retries, messageErr) {
		return retriesExceeded
	}
	maxRetries := m.Options.MaxRetries
	if retries >= maxRetries {
		return retriesExceeded
	}

	newMessage := deepcopy.DeepCopy(m).(*messages.Message)

	newMessage.Options.Retries += 1
	minBackoff := m.Options.MinBackoff
	maxBackoff := m.Options.MaxBackoff

	delay := util.ComputeBackoff(retries, minBackoff, maxBackoff, true)
	newMessage.WithOPtions(messages.MessageDelay(delay))
	ee := b.Enqueue(ctx, newMessage)
	if ee != nil {
		return ee
	}
	return nil
}

func (r *Retries) Name() string {
	return consts.RetriesMiddleware
}
