package middleware

import (
	"context"
	"errors"

	mapSet "github.com/deckarep/golang-set"
	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/task"
	"github.com/fakerjeff/go_async_tasks/messages"
)

var InvalidMiddleware = errors.New("invalid middleware")

const (
	BeforeEnqueue = iota + 1
	AfterEnqueue
	BeforeAck
	AfterAck
	BeforeNack
	AfterNack
	BeforeProcessMessage
	AfterProcessMessage
)

type IMiddleware interface {
	Phase() []int
	Name() string
}

type IMiddlewareHolder interface {
	AddMiddleware(middlewares ...IMiddleware) error
	HasMiddleware(name string) bool
	IBeforeEnqueueMiddleWare
	IAfterEnqueueMiddleWare
	IBeforeAckMiddleware
	IAfterAckMiddleware
	IBeforeNackMiddleware
	IAfterNackMiddleware
	IBeforeProcessMessageMiddleware
	IAfterProcessMessageMiddleware
}

func Hodler(obj interface{}) (IMiddlewareHolder, bool) {
	if holder, ok := obj.(IMiddlewareHolder); ok {
		return holder, true
	} else {
		return nil, false
	}
}

type IBeforeEnqueueMiddleWare interface {
	BeforeEnqueue(ctx context.Context, broker broker.IBroker, message *messages.Message) error
}

type IAfterEnqueueMiddleWare interface {
	AfterEnqueue(ctx context.Context, broker broker.IBroker, message *messages.Message) error
}

type IBeforeAckMiddleware interface {
	BeforeAck(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error
}

type IAfterAckMiddleware interface {
	AfterAck(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error
}

type IBeforeNackMiddleware interface {
	BeforeNack(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error
}

type IAfterNackMiddleware interface {
	AfterNack(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error
}

type IBeforeProcessMessageMiddleware interface {
	BeforeProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error
}

type IAfterProcessMessageMiddleware interface {
	AfterProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage, result []*task.Result) error
}

type summary struct {
	BeforeEnqueues        []IBeforeEnqueueMiddleWare
	AfterEnqueues         []IAfterEnqueueMiddleWare
	BeforeAcks            []IBeforeAckMiddleware
	AfterAcks             []IAfterAckMiddleware
	BeforeNacks           []IBeforeNackMiddleware
	AfterNacks            []IAfterNackMiddleware
	BeforeProcessMessages []IBeforeProcessMessageMiddleware
	AfterProcessMessages  []IAfterProcessMessageMiddleware
}

func newSummary() *summary {
	return &summary{
		BeforeEnqueues:        []IBeforeEnqueueMiddleWare{},
		AfterEnqueues:         []IAfterEnqueueMiddleWare{},
		BeforeAcks:            []IBeforeAckMiddleware{},
		AfterAcks:             []IAfterAckMiddleware{},
		BeforeNacks:           []IBeforeNackMiddleware{},
		AfterNacks:            []IAfterNackMiddleware{},
		BeforeProcessMessages: []IBeforeProcessMessageMiddleware{},
		AfterProcessMessages:  []IAfterProcessMessageMiddleware{},
	}
}

func (b *summary) BeforeEnqueue(ctx context.Context, broker broker.IBroker, message *messages.Message) error {
	for _, m := range b.BeforeEnqueues {
		err := m.BeforeEnqueue(ctx, broker, message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *summary) AfterEnqueue(ctx context.Context, broker broker.IBroker, message *messages.Message) error {
	for _, m := range b.AfterEnqueues {
		err := m.AfterEnqueue(ctx, broker, message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *summary) BeforeAck(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error {
	for _, m := range b.BeforeAcks {
		err := m.BeforeAck(ctx, broker, message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *summary) AfterAck(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error {
	for _, m := range b.AfterAcks {
		err := m.AfterAck(ctx, broker, message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *summary) BeforeNack(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error {
	for _, m := range b.BeforeNacks {
		err := m.BeforeNack(ctx, broker, message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *summary) AfterNack(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error {
	for _, m := range b.AfterNacks {
		err := m.AfterNack(ctx, broker, message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *summary) BeforeProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error {
	for _, m := range b.BeforeProcessMessages {
		err := m.BeforeProcessMessage(ctx, broker, message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *summary) AfterProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage, result []*task.Result) error {
	for _, m := range b.AfterProcessMessages {
		err := m.AfterProcessMessage(ctx, broker, message, result)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *summary) Add(phase int, middleware IMiddleware) error {
	switch phase {
	case BeforeEnqueue:
		m, ok := middleware.(IBeforeEnqueueMiddleWare)
		if ok {
			b.BeforeEnqueues = append(b.BeforeEnqueues, m)
			return nil
		} else {
			return InvalidMiddleware
		}
	case AfterEnqueue:
		m, ok := middleware.(IAfterEnqueueMiddleWare)
		if ok {
			b.AfterEnqueues = append(b.AfterEnqueues, m)
			return nil
		} else {
			return InvalidMiddleware
		}
	case BeforeAck:
		m, ok := middleware.(IBeforeAckMiddleware)
		if ok {
			b.BeforeAcks = append(b.BeforeAcks, m)
			return nil
		} else {
			return InvalidMiddleware
		}
	case AfterAck:
		m, ok := middleware.(IAfterAckMiddleware)
		if ok {
			b.AfterAcks = append(b.AfterAcks, m)
			return nil
		} else {
			return InvalidMiddleware
		}
	case BeforeNack:
		m, ok := middleware.(IBeforeNackMiddleware)
		if ok {
			b.BeforeNacks = append(b.BeforeNacks, m)
			return nil
		} else {
			return InvalidMiddleware
		}
	case AfterNack:
		m, ok := middleware.(IAfterNackMiddleware)
		if ok {
			b.AfterNacks = append(b.AfterNacks, m)
			return nil
		} else {
			return InvalidMiddleware
		}
	case BeforeProcessMessage:
		m, ok := middleware.(IBeforeProcessMessageMiddleware)
		if ok {
			b.BeforeProcessMessages = append(b.BeforeProcessMessages, m)
			return nil
		} else {
			return InvalidMiddleware
		}
	case AfterProcessMessage:
		m, ok := middleware.(IAfterProcessMessageMiddleware)
		if ok {
			b.AfterProcessMessages = append(b.AfterProcessMessages, m)
			return nil
		} else {
			return InvalidMiddleware
		}
	default:
		return InvalidMiddleware
	}
}

type CommonMiddleware struct {
	middlewares mapSet.Set
	summary     *summary
}

func NewCommonMiddleware() *CommonMiddleware {
	return &CommonMiddleware{
		middlewares: mapSet.NewSet(),
		summary:     newSummary(),
	}
}

func (m *CommonMiddleware) AddMiddleware(middlewares ...IMiddleware) error {
	for _, mid := range middlewares {
		m.middlewares.Add(mid.Name())

		for _, phase := range mid.Phase() {
			err := m.summary.Add(phase, mid)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *CommonMiddleware) HasMiddleware(name string) bool {
	return m.middlewares.Contains(name)
}

func (m *CommonMiddleware) BeforeEnqueue(ctx context.Context, broker broker.IBroker, message *messages.Message) error {
	return m.summary.BeforeEnqueue(ctx, broker, message)
}

func (m *CommonMiddleware) AfterEnqueue(ctx context.Context, broker broker.IBroker, message *messages.Message) error {
	return m.summary.AfterEnqueue(ctx, broker, message)
}

func (m *CommonMiddleware) BeforeAck(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error {
	return m.summary.BeforeAck(ctx, broker, message)
}

func (m *CommonMiddleware) AfterAck(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error {
	return m.summary.AfterAck(ctx, broker, message)
}

func (m *CommonMiddleware) BeforeNack(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error {
	return m.summary.BeforeNack(ctx, broker, message)
}

func (m *CommonMiddleware) AfterNack(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error {
	return m.summary.AfterNack(ctx, broker, message)
}

func (m *CommonMiddleware) BeforeProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage) error {
	return m.summary.BeforeProcessMessage(ctx, broker, message)
}

func (m *CommonMiddleware) AfterProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage, result []*task.Result) error {
	return m.summary.AfterProcessMessage(ctx, broker, message, result)
}
