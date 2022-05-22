package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/fakerjeff/go_async_tasks/consts"

	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/log"
	"github.com/fakerjeff/go_async_tasks/iface/middleware"
	"github.com/fakerjeff/go_async_tasks/iface/task"
	"github.com/fakerjeff/go_async_tasks/util/pq"
)

var workProcessClosed = errors.New("work process closed")

type workProcess struct {
	id         string
	broker     broker.IBroker
	workQueue  *pq.ThreadSafePriorityQueue
	consumers  map[string]*consumerProcess
	sendCtx    context.Context
	logger     log.Logger
	customTask task.ITask
	close      chan int
}

func newWorkProcess(id string, broker broker.IBroker, workQueue *pq.ThreadSafePriorityQueue, consumers map[string]*consumerProcess, logger log.Logger, customTask task.ITask) *workProcess {
	return &workProcess{
		id:         id,
		broker:     broker,
		workQueue:  workQueue,
		consumers:  consumers,
		sendCtx:    context.WithValue(context.Background(), "name", id),
		customTask: customTask,
		logger:     logger,
		close:      make(chan int, 1),
	}
}

func (w *workProcess) Process() error {
	w.logger.Info(fmt.Sprint("start process...."))
	for {
		select {
		case <-w.close:
			return workProcessClosed
		case <-w.workQueue.PushNotify():
			if message := w.workQueue.Pop(); message != nil {
				m := message.Value.(consumer.IConsumerMessage)
				w.logger.Info(fmt.Sprint("received a message"), "message", m.Message())
				err := w.processMessage(m)
				if err != nil {
					w.logger.Error("encountered an error while process task", "err", err, "message", m.Message())
				}
				w.logger.Info("worker process done", "message", m.Message())
			}
		}
	}
}

func (w *workProcess) nack(message consumer.IConsumerMessage) {
	m := message.Message()
	w.consumers[m.QueueName].postMessageChan <- &postMessageSignal{
		message: message,
		opType:  nack,
	}
}

func (w *workProcess) ack(message consumer.IConsumerMessage) {
	m := message.Message()
	w.consumers[m.QueueName].postMessageChan <- &postMessageSignal{
		message: message,
		opType:  ack,
	}
}

func (w *workProcess) processMessage(message consumer.IConsumerMessage) error {
	holder, ok := middleware.Hodler(w.broker)
	if ok && holder.HasMiddleware(consts.CurrentMessageMiddleware) {
		message.SetWorker(w.id)
	}
	var err error
	if ok {
		err = holder.BeforeProcessMessage(w.sendCtx, w.broker, message)
		if err != nil {
			message.WithError(err)
			message.Fail()
			w.nack(message)
			return err
		}
	}
	m := message.Message()
	var t task.ITask

	if w.customTask != nil {
		t = w.customTask
	} else {
		t, err = w.broker.GetTask(m.TaskName)
	}

	if err != nil {
		message.WithError(err)
		message.Fail()
		w.nack(message)
		return err
	}
	res, e := t.Call(m.TaskArgs)
	if e != nil {
		message.WithError(e)
		message.Fail()
	}

	if ok {
		err = holder.AfterProcessMessage(w.sendCtx, w.broker, message, res)
		if err != nil {
			message.WithError(err)
			message.Fail()
			w.nack(message)
			return err
		}
	}

	if e != nil {
		w.nack(message)
	} else {
		w.ack(message)
	}
	return nil
}

func (w *workProcess) Close() {
	w.logger.Debug(fmt.Sprintf("worker closed"))
}
