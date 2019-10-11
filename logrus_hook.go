/*
@Time : 2019-10-11 10:27
@Author : zr
*/
package golog

import (
	"github.com/sirupsen/logrus"
	"time"
)

type LogrusHook struct {
	collector *Collector
	levels    []logrus.Level
}

func newLogrusHook(collector *Collector) *LogrusHook {
	hook := &LogrusHook{}
	hook.collector = collector
	for _, v := range logrus.AllLevels {
		level := levelFromLogrusLevel(v)
		if level >= hook.collector.logLevel {
			hook.levels = append(hook.levels, v)
		}
	}

	return hook
}
func levelFromLogrusLevel(logrusLevel logrus.Level) (level LogLevel) {
	switch logrusLevel {
	case logrus.DebugLevel:
		level = LogLevelDebug
	case logrus.InfoLevel:
		level = LogLevelInfo
	case logrus.WarnLevel:
		level = LogLevelWarn
	case logrus.PanicLevel:
		level = LogLevelPanic
	case logrus.ErrorLevel:
		level = LogLevelError
	case logrus.FatalLevel:
		level = LogLevelFatal
	}
	return
}

func logFromEntry(entry *logrus.Entry) (row *Log) {
	row = &Log{
		Msg: entry.Message,
	}

	if execTime, ok := entry.Data["execTime"]; ok {
		row.ExecTime = int(execTime.(time.Duration))
	}
	row.Time = entry.Time
	if trace, ok := entry.Data["trace"]; ok {
		row.Trace = trace.(string)
	}
	if optUserId, ok := entry.Data["optUserId"]; ok {
		row.OptUserId = optUserId.(int)
	}
	if src, ok := entry.Data["src"]; ok {
		row.Trace = src.(string) + "/" + row.Trace
	}
	if code, ok := entry.Data["code"]; ok {
		row.Code = code.(int)
	}
	if paramsD, ok := entry.Data["params"]; ok {
		row.Msg = paramsD.(string) + "|||" + row.Msg
	}

	row.Level = levelFromLogrusLevel(entry.Level)

	return
}

func (l LogrusHook) Fire(entry *logrus.Entry) error {
	log := logFromEntry(entry)
	l.collector.logChan <- log
	return nil
}
func (l LogrusHook) Levels() []logrus.Level {
	return l.levels
}
