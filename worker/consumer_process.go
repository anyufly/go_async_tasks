package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fakerjeff/go_async_tasks/errr"
	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/log"
	"github.com/fakerjeff/go_async_tasks/iface/middleware"
	"github.com/fakerjeff/go_async_tasks/messages"
	"github.com/fakerjeff/go_async_tasks/util"
	"github.com/fakerjeff/go_async_tasks/util/deepcopy"
	"github.com/fakerjeff/go_async_tasks/util/pq"
)

const (
	ack = iota + 1
	nack
)

const DefaultDelayQueueSize = 25

var consumerProcessClosed = errors.New("consumer process closed")

type postMessageSignal struct {
	message consumer.IConsumerMessage
	opType  int
}

type consumerProcess struct {
	id              string
	queueName       string
	prefetch        int
	broker          broker.IBroker
	consumer        consumer.IConsumer
	workQueue       *pq.ThreadSafePriorityQueue
	delayQueue      *pq.ThreadSafeDelayQueue
	logger          log.Logger
	close           chan int
	sendCtx         context.Context
	reConnChan      chan struct{}
	postMessageChan chan *postMessageSignal
	messageChan     chan interface{}
	reConnInterval  time.Duration
	dlxConsumer     bool
}

func newConsumerProcess(id string, queueName string, prefetch int, broker broker.IBroker, workQueue *pq.ThreadSafePriorityQueue, logger log.Logger, reConnInterval time.Duration, dlxConsumer bool) *consumerProcess {
	return &consumerProcess{
		id:              id,
		queueName:       queueName,
		prefetch:        prefetch,
		broker:          broker,
		workQueue:       workQueue,
		logger:          logger,
		reConnChan:      make(chan struct{}, 1),
		close:           make(chan int, 1),
		postMessageChan: make(chan *postMessageSignal, 1),
		sendCtx:         context.WithValue(context.Background(), "name", id),
		reConnInterval:  reConnInterval,
		dlxConsumer:     dlxConsumer,
	}
}

func (p *consumerProcess) reConnect() chan interface{} {
	connectNotify := make(chan interface{})
	go func() {
		for {
			select {
			case <-p.close:
				connectNotify <- consumerProcessClosed
				return
			default:
				if p.consumer == nil {
					c, err := p.broker.Consume(p.sendCtx, p.queueName, p.prefetch)
					if err != nil {
						p.logger.Info(fmt.Sprintf("try reconnect in %d second...", p.reConnInterval/time.Second))
						time.Sleep(p.reConnInterval)
						continue
					}
					messageChan, e := c.Consume()
					if e != nil {
						p.logger.Info(fmt.Sprintf("try reconnect in %d second...", p.reConnInterval/time.Second))
						time.Sleep(p.reConnInterval)
						continue
					}
					p.consumer = c
					p.messageChan = messageChan
				}
				close(connectNotify)
				return
			}
		}
	}()
	return connectNotify
}

func (p *consumerProcess) waitReconnect() error {
	p.logger.Debug("wait re-connect...")
	connNotify := p.reConnect()
	n := <-connNotify
	if e, ok := n.(error); ok {
		return e
	}
	p.logger.Info("connect ready")
	return nil
}

func (p *consumerProcess) initAndReconnect() error {
	p.consumer = nil
	p.messageChan = nil
	if p.delayQueue != nil {
		p.delayQueue.Close()
	}
	p.delayQueue = pq.NewThreadSafeDelayQueue(DefaultDelayQueueSize)
	return p.waitReconnect()
}

func (p *consumerProcess) Process() error {
	p.logger.Info(fmt.Sprint("start consuming...."))
	err := p.initAndReconnect()
	if err != nil {
		return err
	}

	for {

		select {
		case <-p.close:
			return consumerProcessClosed
		case <-p.reConnChan:
			ee := p.initAndReconnect()
			if ee != nil {
				return ee
			}
		case message := <-p.messageChan:
			if _, ok := message.(errr.ConnectionErr); ok {
				p.logger.Info("consumer connect closed when message delivery")
				p.reConnChan <- struct{}{}
			} else if e, ok := message.(error); ok {
				p.logger.Error("message consume failed", "message", message.(consumer.IConsumerMessage).Message(), "err", e)
			} else {
				m := message.(consumer.IConsumerMessage)
				p.logger.Info("received msg", "message", m.Message())
				p.handleMassage(m)
			}
		case sig := <-p.postMessageChan:
			p.postMessage(sig.message, sig.opType)
		case i := <-p.delayQueue.Notify():
			item := i.(*pq.Item)
			message := item.Value.(consumer.IConsumerMessage)
			p.handleDelayMessage(message)
		}

	}
}

func (p *consumerProcess) ackMessage(message consumer.IConsumerMessage) error {
	holder, o := middleware.Hodler(p.broker)
	var err error
	if o {
		err = holder.BeforeAck(p.sendCtx, p.broker, message)
		if err != nil {
			p.logger.Error("encountered while before ack message", "taskName", message.Message().TaskName, "err", err)
			return p.nackMessage(message)
		}
	}
	p.logger.Debug("ack message", "message", message.Message())
	err = p.consumer.Ack(message)
	if err != nil {
		if _, ok := err.(errr.ConnectionErr); !ok {
			p.logger.Warn("encountered an error while ack message, it will cause message re-consume", "message", message.Message(), "err", err)
		}
		return err
	}
	if o {
		err = holder.AfterAck(p.sendCtx, p.broker, message)
		if err != nil {
			p.logger.Error("encountered while after ack message", "message", message.Message(), "err", err)
		}
	}

	return nil
}

func (p *consumerProcess) nackMessage(message consumer.IConsumerMessage) error {
	holder, o := middleware.Hodler(p.broker)
	var err error
	if o {
		err = holder.BeforeNack(p.sendCtx, p.broker, message)
		if err != nil {
			p.logger.Error("encountered while before nack message", "message", message.Message(), "err", err)
		}
	}
	p.logger.Debug("nack message", "message", message.Message())
	err = p.consumer.NAck(message)
	if err != nil {
		if _, ok := err.(errr.ConnectionErr); !ok {
			p.logger.Warn("encountered an error while nack message, it will cause message re-consume", "message", message.Message(), "err", err)
		}
	}
	if o {
		err = holder.AfterNack(p.sendCtx, p.broker, message)
		if err != nil {
			p.logger.Error("encountered while after nack message", "message", message.Message(), "err", err)
		}
	}
	return nil
}

func (p *consumerProcess) postMessage(message consumer.IConsumerMessage, opType int) {
	var err error
	switch opType {
	case ack:
		err = p.ackMessage(message)
	case nack:
		err = p.nackMessage(message)
	}
	if err != nil {
		if _, ok := err.(errr.ConnectionErr); ok {
			p.logger.Error("consumer connect closed when post message")
			p.reConnChan <- struct{}{}
			return
		}
		p.logger.Error("encountered an error while post message", "err", err, "message", message.Message())
	}
}

func (p *consumerProcess) handleDelayMessage(message consumer.IConsumerMessage) {
	m := message.Message()
	newMessage := deepcopy.DeepCopy(m).(*messages.Message)
	newMessage.QueueName = util.QName(m.QueueName)
	newMessage.MessageAt = time.Now().Local().UnixNano()
	newMessage.WithOPtions(messages.MessageDelay(time.Duration(0)))
	err := p.broker.Enqueue(p.sendCtx, newMessage)
	if err != nil {
		// 若新消息入队不成功则输出日志，重新入队
		if _, ok := err.(errr.ConnectionErr); ok {
			p.reConnChan <- struct{}{}
			return
		}
		p.logger.Error("process delayed message failed", "err", err, "message", m)
		e := p.consumer.Requeue(message)
		if e != nil {
			p.logger.Error("encountered an error while requeue message", "message", message.Message(), "err", err)
		}
		return
	}
	p.postMessage(message, ack)
}

func (p *consumerProcess) handleMassage(message consumer.IConsumerMessage) {
	m := message.Message()
	if m.IsDelayed() && !p.dlxConsumer {
		if !p.broker.ConsumeDelayDirect() {
			p.delayQueue.Offer(message, m.ETA)
			p.logger.Debug("push message to delay queue", "message", m)
		}
	} else {
		if !p.broker.HasTask(m.TaskName) {
			message.WithError(taskNotFound)
			message.Fail()
			p.postMessage(message, nack)
			return
		}
		p.workQueue.Push(&pq.Item{
			Priority: int64(m.Options.Priority),
			Value:    message,
		})
		p.logger.Debug("push message to work queue", "message", m)
	}
}

func (p *consumerProcess) Close() {

	close(p.close)
	if p.consumer != nil {
		p.consumer.Close()
	}
	if p.delayQueue != nil {
		items := p.delayQueue.ToSlice()
		for _, item := range items {
			message := item.Value.(consumer.IConsumerMessage)
			err := p.consumer.Requeue(message)
			if err != nil {
				p.logger.Error("encountered an error while requeue message", "message", message.Message(), "err", err)
			}
		}
	}
}
