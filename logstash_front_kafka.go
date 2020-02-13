/*
@Time : 2019-11-04 9:31
@Author : zr
*/
package golog

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	topic = "logstash"
)

type logstashStatus int

const (
	logstashStatusInit logstashStatus = iota
	logstashStatusKafkaConnected
	logstashStatusKafkaDisconnected
)

type LogstashFrontKafka struct {
	producer sarama.SyncProducer
	logChan  chan *logrus.Entry
	logFile  *os.File
	config   *LogstashFrontKafkaConfig
	status   logstashStatus
	ctx      context.Context
	cancel   context.CancelFunc
	sync.Mutex
}

type LogstashFrontKafkaConfig struct {
	//应用名,用于创建ES索引
	AppName string
	//连接Kafka失败时信息储存位置
	FileLogPath string
	//trace字段预设值
	TracePrefix string
	KafkaAddrs  []string
}

func NewLogstashFrontKafkaConfig() *LogstashFrontKafkaConfig {
	return &LogstashFrontKafkaConfig{}
}

func NewLogstashFrontKafka(config *LogstashFrontKafkaConfig) *LogstashFrontKafka {
	r := &LogstashFrontKafka{}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	r.config = config
	if r.config.FileLogPath == "" {
		r.config.FileLogPath = r.config.AppName + ".log"
	}

	var err error

	r.logFile, err = os.OpenFile(config.FileLogPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(err)
	}

	r.logChan = make(chan *logrus.Entry, 300)

	go r.workKeepKafkaConnect()

	go func() {
		for {
			r.write()
		}
	}()

	return r
}

func (l *LogstashFrontKafka) workKeepKafkaConnect() {
	var firstRestartTime time.Time
	restartFail := false
	for {
		select {
		case <-l.ctx.Done():
			return
		default:
			l.Lock()
			if l.status != logstashStatusKafkaConnected && len(l.config.KafkaAddrs) > 0 {
				//第一次尝试连接
				if !restartFail {
					firstRestartTime = time.Now()
				}

				l.Unlock()
				err := l.restartKafka()
				if err != nil {
					//第一次失败
					if !restartFail {
						logrus.Error("[golog] kafka connect fail\n" + err.Error())
					}
					restartFail = true
				} else {
					//重试数次后成功
					if restartFail {
						restartDuration := time.Now().Sub(firstRestartTime)
						logrus.Errorf("[golog] kafka reconnect for %v", restartDuration)
					}

					restartFail = false
				}
			} else {
				l.Unlock()
				time.Sleep(time.Second)
			}
		}
	}
}
func (l *LogstashFrontKafka) RestartKafka() (err error) {
	l.Lock()
	defer l.Unlock()
	return l.restartKafka()
}

func (l *LogstashFrontKafka) restartKafka() (err error) {
	l.status = logstashStatusKafkaDisconnected
	if l.producer != nil {
		l.producer.Close()
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.DialTimeout = time.Second * 1
	saramaConfig.Net.ReadTimeout = time.Second * 1
	saramaConfig.Net.WriteTimeout = time.Second * 1
	saramaConfig.Producer.Timeout = time.Second * 3
	// 等待服务器所有副本都保存成功后的响应
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	// 随机的分区类型：返回一个分区器，该分区器每次选择一个随机分区
	saramaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	// 是否等待成功和失败后的响应
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(l.config.KafkaAddrs, saramaConfig)
	if err != nil {
		return
	}

	l.producer = producer
	l.status = logstashStatusKafkaConnected
	logrus.Info("[golog] kafka connect success")
	return
}

func (l *LogstashFrontKafka) SetKafkaAddrs(kafkaAddrs []string) {
	l.config.KafkaAddrs = kafkaAddrs
	err := l.RestartKafka()
	if err != nil {
		logrus.Errorf("[golog]kafka connect fail\n%s", err)
	}
}

func (l *LogstashFrontKafka) writeToFile(entry *logrus.Entry) {
	logStr, err := entry.String()
	if err != nil {
		fmt.Println("[Error][golog]format entry fail: " + err.Error())
		return
	}

	_, err = fmt.Fprint(l.logFile, logStr)
	if err != nil {
		fmt.Println("[Error][golog]write log to file: " + err.Error())
	}
}
func (l *LogstashFrontKafka) writeToKafka(entry *logrus.Entry) (err error) {
	logBytes, err := l.GetLogrusFormatter().Format(entry)
	if err != nil {
		err = fmt.Errorf("[golog]format entry error\n%w", err)
		return
	}

	//构建发送的消息，
	msg := &sarama.ProducerMessage{
		Partition: int32(0),
		Key:       sarama.StringEncoder("key"),
		//Value:     sarama.StringEncoder(logString),
		Value: sarama.ByteEncoder(logBytes),
		Topic: topic,
	}

	l.Lock()
	defer l.Unlock()
	_, _, err = l.producer.SendMessage(msg)
	if err != nil {
		l.status = logstashStatusKafkaDisconnected
		err = fmt.Errorf("[golog]send to kafka fail\n%w", err)
		return
	}

	return
}

func (l *LogstashFrontKafka) write() {
	defer func() {
		if err := recover(); err != nil {
			errEntry := logrus.WithError(fmt.Errorf("%s", err))
			l.writeToFile(errEntry)
		}
	}()
	entry := <-l.logChan

	if l.status == logstashStatusKafkaConnected {
		err := l.writeToKafka(entry)
		if err != nil {
			logrus.Error(err)
			l.writeToFile(entry)
		}
	} else {
		l.writeToFile(entry)
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
	entryNew := entry.WithField("app", h.core.config.AppName)
	trace := h.core.config.TracePrefix
	if trace_, ok := entry.Data["trace"]; ok {
		if trace_, ok := trace_.(string); ok {
			trace = strings.Join([]string{
				trace, trace_,
			}, "/")
		}
	}
	entryNew.Data["trace"] = trace
	entryNew.Level = entry.Level
	entryNew.Message = entry.Message
	entryNew.Time = entry.Time
	h.core.logChan <- entryNew
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
