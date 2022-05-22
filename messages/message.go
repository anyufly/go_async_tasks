package messages

import (
	"time"

	"github.com/fakerjeff/go_async_tasks/iface/ident"
	"github.com/fakerjeff/go_async_tasks/iface/task"
	"github.com/fakerjeff/go_async_tasks/util/json"
)

type Message struct {
	ID        string         `json:"message_id"`
	QueueName string         `json:"queue_name"`
	TaskName  string         `json:"task_name"`
	TaskArgs  []task.Arg     `json:"task_args"`
	MessageAt int64          `json:"message_at"`
	ETA       int64          `json:"eta"`
	Options   messageOptions `json:"option"`
}

func NewMessage(queueName, taskName string, taskArgs []task.Arg) *Message {
	idMaker := ident.NewUUID4Maker()
	now := time.Now().Local()
	return &Message{
		ID:        idMaker.NewID(),
		QueueName: queueName,
		TaskName:  taskName,
		TaskArgs:  taskArgs,
		MessageAt: now.UnixNano(),
		ETA:       now.UnixNano(),
	}
}

func (m *Message) WithOPtions(opts ...IMessageOption) *Message {
	for _, opt := range opts {
		opt.apply(m)
	}
	return m
}

func (m *Message) IsDelayed() bool {
	return m.Options.Delay > time.Duration(0)
}

func (m *Message) String() string {
	s, _ := json.MarshalToString(m)
	return s
}
