package golog

import (
	"context"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/utils"
	"time"
)

type LoggerGorm struct {
	LogLevel                            logger.LogLevel
	traceStr, traceErrStr, traceWarnStr string
	SlowThreshold                       time.Duration
}

func NewLoggerGorm(slowThreshold time.Duration) logger.Interface {
	var (
		traceStr     = "%s\n[%.3fms] [rows:%v] %s"
		traceWarnStr = "%s %s\n[%.3fms] [rows:%v] %s"
		traceErrStr  = "%s %s\n[%.3fms] [rows:%v] %s"
	)
	return &LoggerGorm{
		SlowThreshold: slowThreshold,
		traceStr:      traceStr,
		traceWarnStr:  traceWarnStr,
		traceErrStr:   traceErrStr,
	}
}

func (l *LoggerGorm) LogMode(level logger.LogLevel) logger.Interface {
	l.LogLevel = level
	switch level {
	case logger.Silent:
		logrus.SetLevel(logrus.FatalLevel)
	case logger.Info:
		logrus.SetLevel(logrus.InfoLevel)
	case logger.Warn:
		logrus.SetLevel(logrus.WarnLevel)
	case logger.Error:
		logrus.SetLevel(logrus.ErrorLevel)
	}
	return l
}

func (l LoggerGorm) Info(ctx context.Context, s string, i ...interface{}) {
	logrus.Infof(s, i...)
}

func (l LoggerGorm) Warn(ctx context.Context, s string, i ...interface{}) {
	logrus.Warnf(s, i...)
}

func (l LoggerGorm) Error(ctx context.Context, s string, i ...interface{}) {
	logrus.Errorf(s, i...)
}

func (l LoggerGorm) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	elapsed := time.Since(begin)
	sql, rows := fc()
	msg := ""
	file := utils.FileWithLineNum()
	entry := logrus.WithFields(logrus.Fields{
		"execTime": elapsed.Milliseconds(),
		"file":     file,
	})
	switch {
	case err != nil && !errors.Is(err, gorm.ErrRecordNotFound):
		if rows == -1 {
			msg = fmt.Sprintf(l.traceErrStr, file, err, float64(elapsed.Nanoseconds())/1e6, "-", sql)
		} else {
			msg = fmt.Sprintf(l.traceErrStr, file, err, float64(elapsed.Nanoseconds())/1e6, rows, sql)
		}
		entry.Error(msg)
	case elapsed > l.SlowThreshold && l.SlowThreshold != 0:
		slowLog := fmt.Sprintf("SLOW SQL >= %v", l.SlowThreshold)
		if rows == -1 {
			msg = fmt.Sprintf(l.traceWarnStr, utils.FileWithLineNum(), slowLog, float64(elapsed.Nanoseconds())/1e6, "-", sql)
		} else {
			msg = fmt.Sprintf(l.traceWarnStr, utils.FileWithLineNum(), slowLog, float64(elapsed.Nanoseconds())/1e6, rows, sql)
		}
		entry.Warn(msg)
	default:
		sql, rows := fc()
		if rows == -1 {
			msg = fmt.Sprintf(l.traceStr, utils.FileWithLineNum(), float64(elapsed.Nanoseconds())/1e6, "-", sql)
		} else {
			msg = fmt.Sprintf(l.traceStr, utils.FileWithLineNum(), float64(elapsed.Nanoseconds())/1e6, rows, sql)
		}
		entry.Debug(msg)
	}
}
