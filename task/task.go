package tasks

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/fakerjeff/go_async_tasks/config"

	"github.com/fakerjeff/go_async_tasks/consts"

	"github.com/fakerjeff/go_async_tasks/errr"

	"github.com/fakerjeff/go_async_tasks/iface/middleware"

	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/log"
	"github.com/fakerjeff/go_async_tasks/iface/task"
	"github.com/fakerjeff/go_async_tasks/messages"
	"github.com/fakerjeff/go_async_tasks/util"
)

var (
	ErrTaskMustBeFunc             = errors.New("task must be a func type")
	ErrTaskReturnsNoValue         = errors.New("task must return at least a single value")
	ErrLastReturnValueMustBeError = errors.New("last return value of a task must be error")
	ErrTaskNotRegister            = errors.New("task not register")
)

func validateTask(task interface{}) error {
	v := reflect.ValueOf(task)
	t := v.Type()

	if t.Kind() != reflect.Func {
		return ErrTaskMustBeFunc
	}

	if t.NumOut() < 1 {
		return ErrTaskReturnsNoValue
	}

	lastReturnType := t.Out(t.NumOut() - 1)
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !lastReturnType.Implements(errorInterface) {
		return ErrLastReturnValueMustBeError
	}

	return nil
}

type Task struct {
	name       string
	queueName  string
	TaskFunc   reflect.Value
	UseContext bool
	sendCtx    context.Context
	callCtx    context.Context
	broker     broker.IBroker
	logger     log.Logger
	Options    taskOption
}

func NewTask(name string, queueName string, taskFunc interface{}, b broker.IBroker) *Task {
	if err := validateTask(taskFunc); err != nil {
		panic(err)
	}
	t := &Task{
		name:      name,
		queueName: queueName,
		TaskFunc:  reflect.ValueOf(taskFunc),
		broker:    b,
		callCtx:   context.Background(),
	}

	taskFuncType := reflect.TypeOf(taskFunc)
	if taskFuncType.NumIn() > 0 {
		arg0Type := taskFuncType.In(0)
		if util.IsContextType(arg0Type) {
			t.UseContext = true
		}
	}

	return t
}

func (t *Task) WithOptions(options ...ITaskOption) *Task {
	for _, option := range options {
		option.apply(t)
	}
	return t
}

func (t *Task) WithSendContext(ctx context.Context) *Task {
	t.sendCtx = ctx
	return t
}

func (t *Task) Broker() broker.IBroker {
	return t.broker
}

func (t *Task) Call(args []task.Arg) (taskResults []*task.Result, err error) {
	defer func() {
		if e := recover(); e != nil {
			taskResults = nil
			err = fmt.Errorf("encountered an error while call task:%s", e)
		}
	}()

	var argValues []reflect.Value

	if argValues, err = t.ReflectArgs(args); err != nil {
		return nil, fmt.Errorf("reflect task args error: %s", err)
	}

	if t.UseContext {
		ctxValue := reflect.ValueOf(t.callCtx)
		argValues = append([]reflect.Value{ctxValue}, argValues...)
	}

	results := t.TaskFunc.Call(argValues)

	if len(results) == 0 {
		return nil, ErrTaskReturnsNoValue
	}

	lastResult := results[len(results)-1]

	if !lastResult.IsNil() {
		errorInterface := reflect.TypeOf((*error)(nil)).Elem()
		if !lastResult.Type().Implements(errorInterface) {
			return nil, ErrLastReturnValueMustBeError
		}

		return nil, lastResult.Interface().(error)
	}

	taskResults = make([]*task.Result, len(results)-1)
	for i := 0; i < len(results)-1; i++ {
		val := results[i].Interface()
		typeStr := reflect.TypeOf(val).String()
		taskResults[i] = &task.Result{
			Type:  typeStr,
			Value: val,
		}
	}
	return taskResults, err
}

func (t *Task) ReflectArgs(args []task.Arg) ([]reflect.Value, error) {
	argValues := make([]reflect.Value, len(args))

	for i, arg := range args {
		argValue, err := util.ReflectValue(arg.Type, arg.Value)
		if err != nil {
			return nil, err
		}
		argValues[i] = argValue
	}

	return argValues, nil
}

func (t *Task) taskQueueName() string {
	if t.queueName == "" {
		return t.name
	}
	return t.queueName
}

func (t *Task) BuildMessage(args []task.Arg, options ...messages.IMessageOption) *messages.Message {
	message := messages.NewMessage(t.taskQueueName(), t.name, args).WithOPtions(options...)
	return message
}

func (t *Task) Send(args []task.Arg, options ...messages.IMessageOption) (*TaskResult, error) {
	message := t.BuildMessage(args, options...)
	if !t.broker.HasTask(t.name) {
		return nil, ErrTaskNotRegister
	}
	if t.sendCtx == nil {
		t.sendCtx = context.Background()
	}
	err := t.broker.Enqueue(t.sendCtx, message)
	if err != nil {
		return nil, err
	}
	return newTaskResult(message.ID, t.broker), nil
}

func (t *Task) Delay(args []task.Arg, time time.Duration, options ...messages.IMessageOption) (*TaskResult, error) {
	options = append(options, messages.MessageDelay(time))
	return t.Send(args, options...)
}

func (t *Task) Name() string {
	return t.name
}

type TaskResult struct {
	messageID string
	broker    broker.IBroker
}

func newTaskResult(messageID string, broker broker.IBroker) *TaskResult {
	return &TaskResult{
		messageID: messageID,
		broker:    broker,
	}
}

func (r *TaskResult) Result(ctx context.Context) ([]*task.Result, error) {
	holder, ok := middleware.Hodler(r.broker)
	if !ok {
		return nil, errr.BrokerNotSupportMiddleware
	}
	if !holder.HasMiddleware(consts.ResultsMiddleware) {
		return nil, errr.ResultMiddlewareNotConfig
	}
	return config.Backend().Get(ctx, r.messageID)
}

func (r *TaskResult) Completed(ctx context.Context) (bool, error) {
	holder, ok := middleware.Hodler(r.broker)
	if !ok {
		return false, errr.BrokerNotSupportMiddleware
	}
	if !holder.HasMiddleware(consts.ResultsMiddleware) {
		return false, errr.ResultMiddlewareNotConfig
	}
	return config.Backend().Completed(ctx, r.messageID)
}

func (r *TaskResult) MessageID() string {
	return r.messageID
}
