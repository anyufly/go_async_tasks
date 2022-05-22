package logger

import (
	"io"
	"time"

	"github.com/fakerjeff/go_async_tasks/consts"
	"github.com/fakerjeff/go_async_tasks/iface/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type DefaultLogger struct {
	logger *zap.SugaredLogger
	config Config
}

type Config struct {
	LogLevel zapcore.Level
	Options  []zap.Option
	Writer   io.Writer
}

var encodeConfig = zapcore.EncoderConfig{
	TimeKey:        "time",
	LevelKey:       "level",
	NameKey:        "name",
	CallerKey:      "caller",
	FunctionKey:    "",
	MessageKey:     "msg",
	StacktraceKey:  "stacktrace",
	LineEnding:     zapcore.DefaultLineEnding,
	EncodeLevel:    zapcore.CapitalLevelEncoder,
	EncodeTime:     timeEncoder,
	EncodeDuration: zapcore.SecondsDurationEncoder,
	EncodeCaller:   zapcore.ShortCallerEncoder,
}

func timeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format(consts.DefaultTimeFmt))
}

func NewDefaultLogger(name string, config Config) *DefaultLogger {
	syncer := zapcore.AddSync(config.Writer)
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encodeConfig), syncer, config.LogLevel)
	return &DefaultLogger{
		logger: zap.New(core, config.Options...).Named(name).Sugar(),
		config: config,
	}
}

func (d *DefaultLogger) Info(msg string, data ...interface{}) {
	d.logger.Infow(msg, data...)
}

func (d *DefaultLogger) Debug(msg string, data ...interface{}) {
	d.logger.Debugw(msg, data...)
}

func (d *DefaultLogger) Error(msg string, data ...interface{}) {
	d.logger.Errorw(msg, data...)
}

func (d *DefaultLogger) Fatal(msg string, data ...interface{}) {
	d.logger.Fatalw(msg, data...)
}

func (d *DefaultLogger) Warn(msg string, data ...interface{}) {
	d.logger.Warnw(msg, data...)
}

func (d *DefaultLogger) FromName(name string, fields ...zap.Field) log.Logger {
	l := NewDefaultLogger(name, d.config)
	nl := l.logger.Desugar().WithOptions(zap.Fields(fields...))
	l.logger = nl.Sugar()
	return l
}
