package middleware

import (
	"context"
	"time"

	"github.com/fakerjeff/go_async_tasks/config"
	"github.com/fakerjeff/go_async_tasks/messages"

	"github.com/fakerjeff/go_async_tasks/consts"

	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/middleware"
	"github.com/fakerjeff/go_async_tasks/iface/task"
)

type Pipeline struct {
}

func NewPipeline() *Pipeline {
	return &Pipeline{}
}

func (p *Pipeline) Phase() []int {
	return []int{
		middleware.AfterProcessMessage,
	}
}

func (p *Pipeline) Name() string {
	return consts.PipelineMiddleware
}

func (p *Pipeline) AfterProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage, result []*task.Result) error {
	messageErr := message.Err()
	if messageErr != nil || message.IsFailed() {
		return messageErr
	}

	m := message.Message()
	pipeTarget := m.Options.PipeTarget
	pipeIngnore := m.Options.PipeIgnore
	pipeID := m.Options.PipelineID
	pipeLast := m.Options.PipeLast

	if pipeTarget != nil {
		nextMessage := pipeTarget
		if !pipeIngnore {
			for _, r := range result {
				nextMessage.TaskArgs = append(nextMessage.TaskArgs, task.Arg{
					Type:  r.Type,
					Value: r.Value,
				})
			}
		}

		if nextMessage.Options.Delay != time.Duration(0) {
			nextMessage.WithOPtions(messages.MessageDelay(nextMessage.Options.Delay))
		}

		ee := broker.Enqueue(ctx, nextMessage)
		if ee != nil {
			return ee
		}
	}
	if pipeLast && pipeID != "" {
		if holder, ok := middleware.Hodler(broker); ok && holder.HasMiddleware(consts.ResultsMiddleware) {
			_ = config.Backend().DeletePipeline(ctx, pipeID)
		}
	}
	return nil
}
