/*
@Time : 2019-10-11 10:12
@Author : zr
*/
package golog

import (
	"github.com/sirupsen/logrus"
	"time"
)

type LogLevel int8

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
	LogLevelFatal
	LogLevelPanic
)

func (l LogLevel) ToLogrusLevel() logrus.Level {
	return logrus.Level(-l + 5)
}

type Log struct {
	Time      time.Time `gorm:"index:idx_log_time"`
	Trace     string    `gorm:"type:varchar(200);index:idx_log_name"`
	Level     LogLevel  `gorm:"index:idx_log_level"`
	Code      int       `gorm:"type:int;index:idx_log_code"`
	OptUserId int
	ExecTime  int
	Msg       string `gorm:"type:varchar(2000)"`
	tableName string `gorm:"-"`
}

func (l Log) TableName() string {
	return l.tableName
}
