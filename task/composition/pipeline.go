package composition

import (
	"container/list"
	"context"

	"github.com/fakerjeff/go_async_tasks/consts"

	"github.com/fakerjeff/go_async_tasks/errr"

	"github.com/fakerjeff/go_async_tasks/iface/middleware"

	"github.com/fakerjeff/go_async_tasks/config"
	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/messages"
)

type Pipeline struct {
	ID            string
	broker        broker.IBroker
	messages      *list.List
	sendCtx       context.Context
	lastMessageID string
}

func NewPipeline(b broker.IBroker, children ...interface{}) *Pipeline {
	pipeline := new(Pipeline)
	pipeline.ID = config.IDMaker().NewID()
	pipeline.broker = b
	pipeline.messages = list.New()

	for _, child := range children {
		switch child.(type) {
		case *Pipeline:
			p := child.(*Pipeline)
			pipeline.messages.PushBackList(p.messages)
		case *messages.Message:
			message := child.(*messages.Message)
			pipeline.messages.PushBack(message)
		default:
			continue
		}
	}

	for ele := pipeline.messages.Front(); ele != nil; ele = ele.Next() {
		message := ele.Value.(*messages.Message)
		if n := ele.Next(); n != nil {
			nextMessage := n.Value.(*messages.Message)
			message.WithOPtions(messages.Pipeline{
				PipelineID: pipeline.ID,
				PipeTarget: nextMessage,
			})
		}

	}
	back := pipeline.messages.Back()
	lastMessage := back.Value.(*messages.Message)
	lastMessage.WithOPtions(messages.Pipeline{
		PipelineID: pipeline.ID,
		PipeLast:   true,
	})
	pipeline.lastMessageID = lastMessage.ID
	return pipeline
}

func (p *Pipeline) WithSendCtx(sendCtx context.Context) *Pipeline {
	p.sendCtx = sendCtx
	return p
}

func (p *Pipeline) Len() int {
	return p.messages.Len()
}

func (p *Pipeline) Completed(ctx context.Context) (bool, error) {
	holder, ok := middleware.Hodler(p.broker)
	if !ok {
		return false, errr.BrokerNotSupportMiddleware
	}
	if !holder.HasMiddleware(consts.ResultsMiddleware) {
		return false, errr.ResultMiddlewareNotConfig
	}
	return config.Backend().PipelineCompleted(ctx, p.ID)
}

func (p *Pipeline) Send() error {
	if p.Len() == 0 {
		return nil
	}

	if p.sendCtx == nil {
		p.sendCtx = context.Background()
	}

	if holder, ok := middleware.Hodler(p.broker); ok && holder.HasMiddleware(consts.ResultsMiddleware) {
		err := config.Backend().SavePipeline(p.sendCtx, p.ID, p.lastMessageID)
		if err != nil {
			return err
		}
	}

	front := p.messages.Front()
	message := front.Value.(*messages.Message)
	return p.broker.Enqueue(p.sendCtx, message)
}
