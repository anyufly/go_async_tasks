package rabbitmq

import (
	"errors"
	"fmt"
	"os"

	"github.com/fakerjeff/go_async_tasks/config"

	mapSet "github.com/deckarep/golang-set"
	"github.com/fakerjeff/go_async_tasks/errr"
	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/log"
	"github.com/fakerjeff/go_async_tasks/logger"
	"github.com/fakerjeff/go_async_tasks/messages"
	"github.com/fakerjeff/go_async_tasks/util"
	"github.com/fakerjeff/go_async_tasks/util/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	wrongMessageType = errors.New("wrong message type")
	notInKnownTags   = errors.New("not in known tags")
	ConsumerClosed   = errors.New("consumer closed")
)

type Consumer struct {
	logger             log.Logger
	connector          *util.RabbitMQConnector
	conn               *amqp.Connection
	channel            *amqp.Channel
	connCloseNotify    chan *amqp.Error
	channelCloseNotify chan *amqp.Error
	queueName          string
	prefetch           int
	knownTags          mapSet.Set
	close              chan int
}

func NewConsumer(queueName string, prefetch int) *Consumer {
	return &Consumer{
		logger: logger.NewDefaultLogger("rabbitmq.Consumer", logger.Config{
			LogLevel: zapcore.InfoLevel,
			Writer:   os.Stdout,
			Options:  []zap.Option{zap.AddCaller(), zap.AddCallerSkip(1)},
		}),
		queueName: queueName,
		prefetch:  prefetch,
		knownTags: mapSet.NewSet(),
		close:     make(chan int, 1),
	}
}

func (r *Consumer) WithLogger(logger log.Logger) *Consumer {
	r.logger = logger
	return r
}

func (r *Consumer) FromConnector(connector *util.RabbitMQConnector) (*Consumer, error) {
	conn, err := connector.Open()
	if err != nil {
		return nil, err
	}

	r.conn = conn
	var channel *amqp.Channel

	channel, err = conn.Channel()
	if err != nil {
		return nil, err
	}
	r.channel = channel

	r.connCloseNotify = make(chan *amqp.Error, 1)
	r.conn.NotifyClose(r.connCloseNotify)
	r.channelCloseNotify = make(chan *amqp.Error, 1)
	r.channel.NotifyClose(r.channelCloseNotify)
	return r, nil
}

func (r *Consumer) Consume() (chan interface{}, error) {
	err := r.channel.Qos(r.prefetch, 0, false)
	if err != nil {
		return nil, err
	}
	var deliveries <-chan amqp.Delivery
	deliveries, err = r.channel.Consume(r.queueName, fmt.Sprintf("ctag-%s-%s", r.queueName, config.IDMaker().NewID()), false, false, false, false, nil)

	if err != nil {
		return nil, err
	}

	select {
	case <-r.close:
		return nil, ConsumerClosed
	case <-r.channelCloseNotify:
		return nil, errr.ErrClosed
	case <-r.connCloseNotify:
		return nil, errr.ErrClosed
	default:
		messageChan := make(chan interface{}, 1)
		go func() {
		Consume:
			for {
				select {
				case <-r.close:
					break Consume
				case <-r.channelCloseNotify:
					messageChan <- errr.ErrClosed
					break Consume
				case <-r.connCloseNotify:
					messageChan <- errr.ErrClosed
					break Consume
				case delivery := <-deliveries:
					r.consumeOne(delivery, messageChan)
				}
			}
		}()

		return messageChan, nil
	}
}

func (r *Consumer) consumeOne(delivery amqp.Delivery, messageChan chan interface{}) {
	m := &messages.Message{}

	err := json.Unmarshal(delivery.Body, m)
	if err != nil {
		// 若消息无法解析直接nack消息
		messageChan <- err
		eer := r.nack(delivery.DeliveryTag)
		if eer != nil {
			// 忽略nack操作异常并记录日志，当发生nack异常时会导致该消息重复被消费
			r.logger.Warn("message nack failed, it will cause message re-consume in next time", "tag", delivery.DeliveryTag, "err", eer)
		}
		return
	}

	rm := &rabbitMQMessage{
		m:   consumer.NewMessageProxy(m),
		tag: delivery.DeliveryTag,
	}
	r.knownTags.Add(delivery.DeliveryTag)
	messageChan <- rm
}

func (r *Consumer) Ack(message consumer.IConsumerMessage) error {
	select {
	case <-r.close:
		return ConsumerClosed
	case <-r.connCloseNotify:
		return errr.ErrClosed
	case <-r.channelCloseNotify:
		return errr.ErrClosed
	default:
		m, ok := message.(*rabbitMQMessage)
		if !ok {
			return wrongMessageType
		}
		tag := m.tag
		err := r.ack(tag)
		return err
	}
}

func (r *Consumer) NAck(message consumer.IConsumerMessage) error {
	select {
	case <-r.close:
		return nil
	default:
		m, ok := message.(*rabbitMQMessage)
		if !ok {
			return wrongMessageType
		}
		tag := m.tag
		err := r.nack(tag)
		return err
	}
}

func (r *Consumer) ack(tag uint64) error {
	if !r.knownTags.Contains(tag) {
		r.logger.Debug("failed to ack message: not in known tags", "tag", tag)
		return notInKnownTags
	}
	r.knownTags.Remove(tag)
	err := r.channel.Ack(tag, false)
	if err != nil {
		r.logger.Debug("failed to ack message: ack error", "tag", tag, "err", err)
		return err
	}
	return nil
}

func (r *Consumer) nack(tag uint64) error {
	if !r.knownTags.Contains(tag) {
		r.logger.Debug("failed to nack message: not in known tags", "tag", tag)
		return notInKnownTags
	}
	r.knownTags.Remove(tag)
	// 将requeue设置为False则消息nack后进入死信队列而不是重新入队
	err := r.channel.Nack(tag, false, false)
	if err != nil {
		r.logger.Debug("failed to nack message: ack error", "tag", tag, "err", err)
		return err
	}
	return nil
}

func (r *Consumer) Requeue(messages ...consumer.IConsumerMessage) error {
	// rabbitMQ会自动回队没有ACK的消息，所以无需处理
	r.logger.Debug("requeue message don't need do anything")
	return nil
}

func (r *Consumer) Close() {
	close(r.close)
	if !r.channel.IsClosed() {
		err := r.channel.Close()
		if err != nil {
			r.logger.Error("encountered an error while closing channel", "err", err)
		}
	}

	if !r.conn.IsClosed() {
		err := r.conn.Close()
		if err != nil {
			r.logger.Error("encountered an error while closing conn", "err", err)
		}
	}
}

type rabbitMQMessage struct {
	m   *consumer.MessageProxy
	tag uint64
}

func (m *rabbitMQMessage) Tag() uint64 {
	return m.tag
}

func (m *rabbitMQMessage) WithError(err error) {
	m.m.WithError(err)
}

func (m *rabbitMQMessage) DeleteError() {
	m.m.DeleteError()
}

func (m *rabbitMQMessage) IsFailed() bool {
	return m.m.IsFailed()
}

func (m *rabbitMQMessage) Fail() {
	m.m.Fail()
}

func (m *rabbitMQMessage) Message() *messages.Message {
	return m.m.Message()
}

func (m *rabbitMQMessage) Err() error {
	return m.m.Err()
}

func (m *rabbitMQMessage) SetWorker(workerID string) {
	m.m.SetWorker(workerID)
}

func (m *rabbitMQMessage) ClearWorker() {
	m.m.ClearWorker()
}

func (m *rabbitMQMessage) WorkerID() string {
	return m.m.WorkerID()
}
