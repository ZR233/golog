/*
@Time : 2019-11-04 9:31
@Author : zr
*/
package golog

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"os"
)

const (
	topic = "logstash"
)

type LogstashFrontKafka struct {
	producer   sarama.SyncProducer
	logChan    chan *logrus.Entry
	fileLogger *logrus.Logger
	appName    string
}

func NewLogstashFrontKafka(appName, fileLogPath string, kafkaAddrs []string) *LogstashFrontKafka {
	r := &LogstashFrontKafka{}
	r.fileLogger = logrus.New()
	r.appName = appName
	src, err := os.OpenFile(fileLogPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, os.ModeAppend)
	if err != nil {
		panic(err)
	}

	r.fileLogger.Out = src
	r.fileLogger.SetFormatter(&logrus.TextFormatter{})

	r.logChan = make(chan *logrus.Entry, 300)
	config := sarama.NewConfig()

	// 等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 随机的分区类型：返回一个分区器，该分区器每次选择一个随机分区
	config.Producer.Partitioner = sarama.NewManualPartitioner
	// 是否等待成功和失败后的响应
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(kafkaAddrs, config)
	if err != nil {
		r.fileLogger.Panic(err)
	}
	r.producer = producer

	go func() {
		for {
			r.write()
		}
	}()

	return r
}

func (l *LogstashFrontKafka) write() {
	defer func() {
		if err := recover(); err != nil {
			l.fileLogger.Warn(err)
		}
	}()
	entry := <-l.logChan
	logString, err := entry.String()
	if err != nil {
		l.fileLogger.Warn(err)
	}

	//构建发送的消息，
	msg := &sarama.ProducerMessage{
		Partition: int32(0),
		Key:       sarama.StringEncoder("key"),
		Value:     sarama.StringEncoder(logString),
		Topic:     topic,
	}

	_, _, err = l.producer.SendMessage(msg)
	if err != nil {
		l.fileLogger.Warn(err)
	}
}

func (l *LogstashFrontKafka) GetLogrusFormatter() *logrus.JSONFormatter {
	return &logrus.JSONFormatter{
		TimestampFormat: "2006-01-02T15:04:05.999+08:00",
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "@timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
			logrus.FieldKeyFunc:  "caller",
		},
	}
}

func (l *LogstashFrontKafka) GetLogrusHook() Hook {
	return Hook{l}
}

type Hook struct {
	core *LogstashFrontKafka
}

func (h Hook) Fire(entry *logrus.Entry) error {
	entry = entry.WithField("app", h.core.appName)
	h.core.logChan <- entry
	return nil
}
func (h Hook) Levels() []logrus.Level {
	levels := []logrus.Level{
		logrus.DebugLevel,
		logrus.InfoLevel,
		logrus.WarnLevel,
		logrus.PanicLevel,
		logrus.ErrorLevel,
		logrus.FatalLevel,
	}
	return levels
}
