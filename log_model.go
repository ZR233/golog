/*
@Time : 2019-10-11 10:12
@Author : zr
*/
package golog

import "time"

type LogLevel int8

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelPanic
	LogLevelError
	LogLevelFatal
)

type Log struct {
	Time      time.Time `gorm:"index:idx_log_time"`
	Trace     string    `gorm:"type:varchar(200);index:idx_log_name"`
	Level     LogLevel  `gorm:"index:idx_log_level"`
	Code      int       `gorm:"type:int;index:idx_log_code"`
	OptUserId int
	ExecTime  int
	Msg       string `gorm:"type:varchar(400)"`
}

func (Log) TableName() string {
	return "s_log"
}
