package log

import "go.uber.org/zap"

type Logger interface {
	Info(msg string, data ...interface{})
	Debug(msg string, data ...interface{})
	Warn(msg string, data ...interface{})
	Error(msg string, data ...interface{})
	Fatal(msg string, data ...interface{})
	FromName(name string, fields ...zap.Field) Logger
}
