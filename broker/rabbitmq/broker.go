package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	mapSet "github.com/deckarep/golang-set"
	"github.com/fakerjeff/go_async_tasks/consumer/rabbitmq"
	"github.com/fakerjeff/go_async_tasks/errr"
	"github.com/fakerjeff/go_async_tasks/iface/consumer"
	"github.com/fakerjeff/go_async_tasks/iface/log"
	"github.com/fakerjeff/go_async_tasks/iface/middleware"
	"github.com/fakerjeff/go_async_tasks/iface/task"
	"github.com/fakerjeff/go_async_tasks/logger"
	"github.com/fakerjeff/go_async_tasks/messages"
	"github.com/fakerjeff/go_async_tasks/util"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const DefaultContextName = "main-goroutine"

type invalidConnName struct {
	connName string
}

func (i invalidConnName) Error() string {
	return fmt.Sprintf("connect named with %s not found in broker", i.connName)
}

type publishNack struct {
	deliveryTag uint64
}

func (p publishNack) Error() string {
	return fmt.Sprintf("publish msg not ack, deliveryTag:%d", p.deliveryTag)
}

type contextDone struct {
	err error
}

func (c *contextDone) Error() string {
	return c.err.Error()
}

var (
	maxRetriedDeclare = errors.New("max retried declare")
	maxRetriedEnqueue = errors.New("max retried enqueue")
	taskNotRegister   = errors.New("task not register")
	invalidTask       = errors.New("invalid task")
	BrokerClosed      = errors.New("broker closed")
)

const (
	DefaultMaxDeclareAttempts = 2                    // 声明队列重试次数
	DefaultMaxEnqueueAttempts = 6                    // 入队重试次数
	DefaultDeadMessageTTL     = 7 * 24 * 3600 * 1000 // 死信队列存活时间 单位ms
)

type BrokerConnection struct {
	conn            *amqp.Connection
	notifyConnClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	channel         *amqp.Channel
	notifyChanClose chan *amqp.Error
}

type Broker struct {
	url                string
	options            brokerOptions
	connections        map[string]*BrokerConnection
	queues             mapSet.Set
	delayQueues        mapSet.Set
	pendingQueues      mapSet.Set
	logger             log.Logger
	close              chan int
	connMutex          sync.Mutex
	maxDeclareAttempts uint
	maxEnqueueAttempts uint
	deadMessageTTL     int32
	tasks              *sync.Map
	*middleware.CommonMiddleware
}

func NewBroker(url string, maxDeclareAttempts uint, maxEnqueueAttempts uint, deadMessageTTL int32) *Broker {
	if maxDeclareAttempts <= 0 {
		maxDeclareAttempts = DefaultMaxDeclareAttempts
	}

	if maxEnqueueAttempts <= 0 {
		maxEnqueueAttempts = DefaultMaxEnqueueAttempts
	}

	if deadMessageTTL <= 0 {
		deadMessageTTL = DefaultDeadMessageTTL
	}

	b := &Broker{
		url:                url,
		connections:        map[string]*BrokerConnection{},
		queues:             mapSet.NewSet(),
		delayQueues:        mapSet.NewSet(),
		pendingQueues:      mapSet.NewSet(),
		maxDeclareAttempts: maxDeclareAttempts,
		maxEnqueueAttempts: maxEnqueueAttempts,
		deadMessageTTL:     deadMessageTTL,
		tasks:              new(sync.Map),
		close:              make(chan int, 1),
		logger: logger.NewDefaultLogger("rabbitmq.Broker", logger.Config{
			LogLevel: zapcore.InfoLevel,
			Writer:   os.Stdout,
			Options:  []zap.Option{zap.AddCaller(), zap.AddCallerSkip(1)},
		}),
	}
	b.CommonMiddleware = middleware.NewCommonMiddleware()
	return b
}

func (r *Broker) WithLogger(logger log.Logger) *Broker {
	r.logger = logger
	return r
}

func (r *Broker) WithOptions(opts ...IBrokerOption) *Broker {
	for _, opt := range opts {
		opt.apply(r)
	}
	return r
}

func (r *Broker) connection(ctx context.Context) (*BrokerConnection, error) {
	select {
	case <-r.close:
		return nil, nil
	case <-ctx.Done():
		return nil, &contextDone{err: ctx.Err()}
	default:
		return r.conn(ctx)
	}
}

func (r *Broker) getCtxName(ctx context.Context) string {
	name := ctx.Value("name")
	if name == nil {
		return DefaultContextName
	}
	return name.(string)
}

func (r *Broker) conn(ctx context.Context) (*BrokerConnection, error) {
	r.connMutex.Lock()
	defer r.connMutex.Unlock()
	c, ok := r.connections[r.getCtxName(ctx)]
	if !ok {
		conn, err := util.NewRabbitMQConnector(r.url).WithOptions((*util.TlsOption)(r.options.tlsConfig)).Open()
		if err != nil {
			return nil, err
		}
		ch, err := conn.Channel()

		if err != nil {
			return nil, err
		}

		c = &BrokerConnection{
			conn:            conn,
			notifyConnClose: make(chan *amqp.Error, 1),
			channel:         ch,
			notifyChanClose: make(chan *amqp.Error, 1),
		}

		if r.options.confirm {
			err := ch.Confirm(false)
			if err != nil {
				return nil, err
			}
			notifyConfirm := make(chan amqp.Confirmation, 1)
			c.notifyConfirm = notifyConfirm
			ch.NotifyPublish(notifyConfirm)
		}

		r.connections[r.getCtxName(ctx)] = c
	}
	return c, nil
}

func (r *Broker) removeConnection(ctx context.Context) error {
	select {
	case <-r.close:
		return BrokerClosed
	case <-ctx.Done():
		return &contextDone{err: ctx.Err()}
	default:
		return r.removeConn(ctx)
	}
}

func (r *Broker) removeConn(ctx context.Context) error {
	r.connMutex.Lock()
	defer r.connMutex.Unlock()
	c, ok := r.connections[r.getCtxName(ctx)]

	if !ok {
		return invalidConnName{
			connName: r.getCtxName(ctx),
		}
	}

	select {
	case <-r.close:
		return BrokerClosed
	case <-ctx.Done():
		return &contextDone{err: ctx.Err()}
	case <-c.notifyConnClose:
		delete(r.connections, r.getCtxName(ctx))
		return errr.ErrClosed
	case <-c.notifyChanClose:
		delete(r.connections, r.getCtxName(ctx))
		if !c.conn.IsClosed() {
			err := c.conn.Close()
			if err != nil {
				return err
			}
		}
		return errr.ErrClosed
	default:
		delete(r.connections, r.getCtxName(ctx))
		if !c.channel.IsClosed() {
			err := c.channel.Close()
			if err != nil {
				return err
			}
		}
		if !c.conn.IsClosed() {
			err := c.conn.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func (r *Broker) Close() {
	close(r.close)
	for connName, c := range r.connections {
		if !c.channel.IsClosed() {
			err := c.channel.Close()
			if err != nil {
				r.logger.Error("encountered an error while closing channel", "connName", connName, "error", err)
			}
		}

		if !c.conn.IsClosed() {
			err := c.conn.Close()
			if err != nil {
				r.logger.Error("encountered an error while closing connection", "connName", connName, "error", err)
			}
		}
		delete(r.connections, connName)
	}
	r.logger.Debug("Channels and connections closed.")
}

func (r *Broker) Consume(ctx context.Context, queueName string, prefetch int) (consumer.IConsumer, error) {
	err := r.declareQueue(ctx, queueName)
	if err != nil {
		return nil, err
	}
	return rabbitmq.NewConsumer(queueName, prefetch).FromConnector(util.NewRabbitMQConnector(r.url).WithOptions((*util.TlsOption)(r.options.tlsConfig)))
}

func (r *Broker) buildQueueArgument(queueName string) amqp.Table {
	arg := amqp.Table{}
	if r.options.maxPriority > 0 {
		arg["x-max-priority"] = r.options.maxPriority
	}
	arg["x-dead-letter-exchange"] = ""
	arg["x-dead-letter-routing-key"] = util.XQName(queueName)
	return arg
}

func (r *Broker) buildQueueArgumentWithPlugin(queueName string) amqp.Table {
	arg := amqp.Table{}
	if r.options.maxPriority > 0 {
		arg["x-max-priority"] = r.options.maxPriority
	}
	arg["x-dead-letter-exchange"] = ""
	arg["x-dead-letter-routing-key"] = util.XQName(queueName)
	arg["x-delayed-type"] = "direct"
	return arg
}

func (r *Broker) declareQueue(ctx context.Context, queueName string) error {
	if !r.queues.Contains(queueName) {
		r.queues.Add(queueName)
		r.pendingQueues.Add(queueName)
		delayQueueName := util.DQName(queueName)
		r.delayQueues.Add(delayQueueName)
	}

	if r.usePlugin() {
		return r.ensureDeclareQueue(ctx, queueName, r.declareWithPlugin)
	} else {
		return r.ensureDeclareQueue(ctx, queueName, r.declare)
	}
}

func (r *Broker) ensureDeclareQueue(ctx context.Context, queueName string, declareFunc func(c *BrokerConnection, queueName string) error) error {
	errChan := make(chan error, 1)
	attempts := uint(1)
	c := new(BrokerConnection)
	var err error

	for {
		select {
		case <-r.close:
			return BrokerClosed
		case <-ctx.Done():
			return &contextDone{err: ctx.Err()}
		case <-c.notifyConnClose:
			errChan <- errr.ErrClosed
		case <-c.notifyChanClose:
			errChan <- errr.ErrClosed
		case ee := <-errChan:
			r.logger.Error("encountered an error while declare queue", "err", ee, "queueName", queueName)
			if e, ok := ee.(*contextDone); ok {
				return e
			}
			ee = r.removeConnection(ctx)
			if ee != nil {
				if e, ok := ee.(*contextDone); ok {
					return e
				}
				r.logger.Error("encountered an error while remove connection", "err", ee)
			}
			attempts += 1
			if attempts > r.maxDeclareAttempts {
				return maxRetriedDeclare
			}
			continue
		default:
			c, err = r.connection(ctx)
			if err != nil {
				c = new(BrokerConnection)
				errChan <- err
				continue
			}

			if r.pendingQueues.Contains(queueName) {
				err = declareFunc(c, queueName)
				if err != nil {
					errChan <- err
					continue
				}
			}
			return nil
		}
	}
}

func (r *Broker) declareDelayQueueWithPlugin(c *BrokerConnection, queueName string) error {
	// 使用rabbitmq_delayed_message_exchange插件实现
	arg := r.buildQueueArgumentWithPlugin(queueName)
	err := c.channel.ExchangeDeclare("delay.exchange", "x-delayed-message", true, false, false, false, arg)
	if err != nil {
		return err
	}

	_, err = c.channel.QueueDeclare(util.DQName(queueName), true, false, false, false, arg)
	if err != nil {
		return err
	}
	err = c.channel.QueueBind(util.DQName(queueName), util.DQName(queueName), "delay.exchange", false, arg)

	if err != nil {
		return err
	}
	return nil
}

func (r *Broker) declareWithPlugin(c *BrokerConnection, queueName string) error {
	err := r.declareNormalQueue(c, queueName)
	if err != nil {
		return err
	}

	err = r.declareDelayQueueWithPlugin(c, queueName)

	if err != nil {
		return err
	}

	err = r.declareDeadQueue(c, queueName)
	if err != nil {
		return err
	}
	r.pendingQueues.Remove(queueName)

	return nil
}

func (r *Broker) declareNormalQueue(c *BrokerConnection, queueName string) error {
	// 普通队列
	_, err := c.channel.QueueDeclare(queueName, true, false, false, false, r.buildQueueArgument(queueName))
	if err != nil {
		return err
	}
	return nil
}

func (r *Broker) declareDelayQueue(c *BrokerConnection, queueName string) error {
	_, err := c.channel.QueueDeclare(util.DQName(queueName), true, false, false, false, r.buildQueueArgument(queueName))
	if err != nil {
		return err
	}
	return nil
}

func (r *Broker) declareDeadQueue(c *BrokerConnection, queueName string) error {
	// 私信队列
	_, err := c.channel.QueueDeclare(util.XQName(queueName), true, false, false, false, amqp.Table{
		"x-message-ttl": r.deadMessageTTL,
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *Broker) declare(c *BrokerConnection, queueName string) error {
	err := r.declareNormalQueue(c, queueName)
	if err != nil {
		return err
	}

	err = r.declareDelayQueue(c, queueName)
	if err != nil {
		return err
	}

	err = r.declareDeadQueue(c, queueName)
	if err != nil {
		return err
	}
	r.pendingQueues.Remove(queueName)
	return nil
}

func (r *Broker) usePlugin() bool {
	return r.options.usePlugin
}

func (r *Broker) Enqueue(ctx context.Context, message *messages.Message) error {
	err := r.BeforeEnqueue(ctx, r, message)
	if err != nil {
		return err
	}
	queueName := message.QueueName
	err = r.declareQueue(ctx, queueName)
	if err != nil {
		return err
	}

	if message.IsDelayed() {
		message.QueueName = util.DQName(queueName)
	}
	err = r.ensureEnqueue(ctx, message)
	if err != nil {
		return err
	}
	err = r.AfterEnqueue(ctx, r, message)
	if err != nil {
		return err
	}
	return nil
}

func (r *Broker) ensureEnqueue(ctx context.Context, message *messages.Message) error {

	errChan := make(chan error, 1)
	attempts := uint(1)
	c := new(BrokerConnection)
	var err error
	for {
		select {
		case <-r.close:
			return BrokerClosed
		case <-ctx.Done():
			return &contextDone{err: ctx.Err()}
		case <-c.notifyConnClose:
			errChan <- errr.ErrClosed
		case <-c.notifyChanClose:
			errChan <- errr.ErrClosed
		case ee := <-errChan:
			r.logger.Error("encountered an error while enqueue", "err", ee)
			if e, ok := ee.(*contextDone); ok {
				return e
			}
			eer := r.removeConnection(ctx)
			if eer != nil {
				if e, ok := eer.(*contextDone); ok {
					return e
				}
				r.logger.Error("encountered an error while remove connection", "err", eer)
			}
			attempts += 1
			if attempts > r.maxEnqueueAttempts {
				if e, ok := ee.(errr.ConnectionErr); ok {
					return e
				}
				return maxRetriedEnqueue
			}

			continue
		default:
			var body []byte
			body, err = json.Marshal(message)
			if err != nil {
				return err
			}

			c, err = r.connection(ctx)
			if err != nil {
				c = new(BrokerConnection)
				errChan <- err
				continue
			}

			err = c.channel.Publish("", message.QueueName, false, false, amqp.Publishing{
				ContentType:  "application/json",
				Priority:     message.Options.Priority,
				DeliveryMode: amqp.Persistent,
				Body:         body,
			})
			if err != nil {
				errChan <- err
				continue
			}
			if r.options.confirm {
				// 等待消息确认
				tag, nack, eer := func() (uint64, bool, error) {
					for {
						select {
						case <-r.close:
							r.logger.Debug("broker closed when confirm ack message")
							return 0, true, BrokerClosed
						case <-c.notifyConnClose:
							r.logger.Debug("connection closed when confirm ack message")
							return 0, true, errr.ErrClosed
						case <-c.notifyChanClose:
							r.logger.Debug("channel closed when confirm ack message")
							return 0, true, errr.ErrClosed
						case <-ctx.Done():
							return 0, true, &contextDone{err: ctx.Err()}
						case conformation := <-c.notifyConfirm:
							tag := conformation.DeliveryTag
							if conformation.Ack {
								r.logger.Info("message publish ack", "tag", tag, "message", message)
								return tag, false, nil
							} else {
								return tag, true, nil
							}
						}
					}
				}()

				if eer != nil {
					if e, ok := eer.(*contextDone); ok {
						errChan <- e
						continue
					}
					r.logger.Error("encountered error when confirm", "tag", tag, "err", eer)
				}

				if nack {
					r.logger.Error("publishing msg nack", "tag", tag, "err", eer)
					errChan <- publishNack{deliveryTag: tag}
					continue
				}
			}
			return nil
		}
	}
}

func (r *Broker) RegisterTask(tasks ...task.ITask) {
	for _, t := range tasks {
		r.tasks.Store(t.Name(), t)
	}
}

func (r *Broker) GetTask(taskName string) (task.ITask, error) {
	t, ok := r.tasks.Load(taskName)
	if !ok {
		return nil, taskNotRegister
	}
	i, ok := t.(task.ITask)
	if !ok {
		return nil, invalidTask
	}
	return i, nil
}

func (r *Broker) HasTask(taskName string) bool {
	_, ok := r.tasks.Load(taskName)
	return ok
}

func (r *Broker) ConsumeDelayDirect() bool {
	if r.usePlugin() {
		return true
	}
	return false
}
