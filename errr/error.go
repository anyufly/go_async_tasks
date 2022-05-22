package errr

import "errors"

type ConnectionErr string

func (c ConnectionErr) Error() string {
	return string(c)
}

var ErrClosed = ConnectionErr("channel/connection is not open")

var GroupNotCompleted = errors.New("group not completed")

var TaskNotComplete = errors.New("task not Complete")

var ResultMiddlewareNotConfig = errors.New("result middle ware not config")

var BrokerNotSupportMiddleware = errors.New("broker not support middleware")
