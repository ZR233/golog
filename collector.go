/*
@Time : 2019-10-11 10:37
@Author : zr
*/
package golog

import (
	"github.com/sirupsen/logrus"
	"time"
)

type Collector struct {
	logLevel  LogLevel
	logChan   chan *Log
	receivers []Receiver
}

func NewCollector() *Collector {
	c := &Collector{
		logLevel: LogLevelDebug,
	}
	c.logChan = make(chan *Log, 1000)
	return c
}

func (c *Collector) GetLogrusHook() *LogrusHook {
	hook := newLogrusHook(c)
	return hook
}
func (c *Collector) SetLevel(level LogLevel) {
	c.logLevel = level
}

func (c *Collector) AddReceiver(receiver Receiver) {
	c.receivers = append(c.receivers, receiver)
}

func (c *Collector) Log(log *Log) {
	c.logChan <- log
}

func LogrusWithFields(logger *logrus.Logger, endTime, beginTime time.Time, url, src string, userid, code int, params []byte) *logrus.Entry {
	return logger.WithFields(logrus.Fields{
		"execTime":  endTime.Sub(beginTime) / time.Millisecond,
		"trace":     url,
		"time":      beginTime,
		"optUserId": userid,
		"src":       src,
		"code":      code,
		"params":    string(params),
	})
}

func (c *Collector) Run() {
	for {
		logRecord := <-c.logChan

		for _, receiver := range c.receivers {
			err := receiver.Receive(logRecord)
			if err != nil {
				println(err)
			}
		}
	}
}
