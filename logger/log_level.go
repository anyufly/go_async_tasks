package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logLevelMap = map[string]zapcore.Level{
	"info":  zap.InfoLevel,
	"debug": zap.DebugLevel,
	"warn":  zap.WarnLevel,
	"error": zap.ErrorLevel,
}

func GetLogLevel(level string) zapcore.Level {
	if logLevel, ok := logLevelMap[level]; ok {
		return logLevel
	} else {
		return zap.ErrorLevel
	}
}
