package composition

import (
	"context"

	"github.com/fakerjeff/go_async_tasks/consts"

	"github.com/fakerjeff/go_async_tasks/config"
	"github.com/fakerjeff/go_async_tasks/errr"
	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/middleware"
	"github.com/fakerjeff/go_async_tasks/messages"
)

type Group struct {
	ID         string
	broker     broker.IBroker
	children   []interface{}
	messageIDs []string
	callbacks  []*messages.Message
	sendCtx    context.Context
}

func NewGroup(broker broker.IBroker, children ...interface{}) *Group {
	group := &Group{
		ID:        config.IDMaker().NewID(),
		broker:    broker,
		callbacks: make([]*messages.Message, 0),
	}
	group.children = make([]interface{}, 0, len(children))
	for _, child := range children {
		switch child.(type) {
		case *Pipeline:
			p := child.(*Pipeline)
			group.children = append(group.children, child)
			lastMessage := p.messages.Back().Value.(*messages.Message)
			group.messageIDs = append(group.messageIDs, lastMessage.ID)
		case *messages.Message:
			m := child.(*messages.Message)
			group.children = append(group.children, child)
			group.messageIDs = append(group.messageIDs, m.ID)
		default:
			continue
		}
	}
	return group
}

func (g *Group) WithSendCtx(sendCtx context.Context) *Group {
	g.sendCtx = sendCtx
	return g
}

func (g *Group) Len() int {
	return len(g.children)
}

func (g *Group) Send() error {
	if g.sendCtx == nil {
		g.sendCtx = context.Background()
	}

	if holder, ok := middleware.Hodler(g.broker); ok && holder.HasMiddleware(consts.ResultsMiddleware) {
		err := config.Backend().SaveGroup(g.sendCtx, g.ID, g.Len(), g.messageIDs)
		if err != nil {
			return err
		}
	}

	err := config.Barrier().Create(g.ID, g.Len())
	if err != nil {
		return err
	}

	for _, child := range g.children {
		switch child.(type) {
		case *Pipeline:
			p := child.(*Pipeline)
			ele := p.messages.Back()
			m := ele.Value.(*messages.Message)
			if len(g.callbacks) > 0 {
				m.WithOPtions(messages.GroupCallbacks{
					GroupID:        g.ID,
					GroupCallbacks: g.callbacks,
				})
			} else {
				m.WithOPtions(messages.GroupCallbacks{
					GroupID: g.ID,
				})
			}
			p.WithSendCtx(g.sendCtx)
			err := p.Send()
			if err != nil {
				return err
			}
		case *messages.Message:
			m := child.(*messages.Message)
			if len(g.callbacks) > 0 {
				m.WithOPtions(messages.GroupCallbacks{
					GroupID:        g.ID,
					GroupCallbacks: g.callbacks,
				})
			} else {
				m.WithOPtions(messages.GroupCallbacks{
					GroupID: g.ID,
				})
			}
			err := g.broker.Enqueue(g.sendCtx, m)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *Group) AddCallback(messages ...*messages.Message) {
	g.callbacks = append(g.callbacks, messages...)
}

func (g *Group) Completed(ctx context.Context) (bool, error) {
	holder, ok := middleware.Hodler(g.broker)
	if !ok {
		return false, errr.BrokerNotSupportMiddleware
	}
	if !holder.HasMiddleware(consts.ResultsMiddleware) {
		return false, errr.ResultMiddlewareNotConfig
	}
	return config.Backend().GroupCompleted(ctx, g.ID)
}

func (g *Group) CompletedCount(ctx context.Context) (int64, error) {
	holder, ok := middleware.Hodler(g.broker)
	if !ok {
		return 0, errr.BrokerNotSupportMiddleware
	}
	if !holder.HasMiddleware(consts.ResultsMiddleware) {
		return 0, errr.ResultMiddlewareNotConfig
	}
	return config.Backend().GroupCompleteCount(ctx, g.ID)
}
