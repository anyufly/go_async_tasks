package consumer

import (
	"github.com/fakerjeff/go_async_tasks/messages"
)

type IConsumerMessage interface {
	Message() *messages.Message
	WithError(err error)
	Err() error
	DeleteError()
	IsFailed() bool
	SetWorker(workerID string)
	WorkerID() string
	ClearWorker()
	Fail()
}

type MessageProxy struct {
	workerID string
	message  *messages.Message
	failed   bool
	err      error
}

func NewMessageProxy(message *messages.Message) *MessageProxy {
	return &MessageProxy{
		message: message,
	}
}

func (m *MessageProxy) WithError(err error) {
	m.err = err
}

func (m *MessageProxy) Fail() {
	m.failed = true
}

func (m *MessageProxy) DeleteError() {
	m.err = nil
}

func (m *MessageProxy) IsFailed() bool {
	return m.failed
}

func (m *MessageProxy) Message() *messages.Message {
	return m.message
}

func (m *MessageProxy) Err() error {
	return m.err
}

func (m *MessageProxy) SetWorker(workerID string) {
	m.workerID = workerID
}

func (m *MessageProxy) ClearWorker() {
	m.workerID = ""
}

func (m *MessageProxy) WorkerID() string {
	return m.workerID
}

type IConsumer interface {
	Ack(message IConsumerMessage) error
	NAck(message IConsumerMessage) error
	Consume() (chan interface{}, error)
	Requeue(messages ...IConsumerMessage) error
	Close()
}
