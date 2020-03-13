/*
@Time : 2019-11-04 9:31
@Author : zr
*/
package logstash_kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	topic           = "logstash"
	logPrefix       = "[golog]"
	timestampFormat = "2006-01-02T15:04:05.999+08:00"
)

type FrontStatus int

var (
	logstash *FrontKafka
)

func EnableLogrusLogstashKafka(appName, logFile, tracePrefix string, zhHosts []string) {
	cfg := NewLogstashFrontKafkaConfig()
	cfg.AppName = appName
	cfg.FileLogPath = logFile
	cfg.ZKHosts = zhHosts
	cfg.TracePrefix = tracePrefix
	logstash = NewLogstashFrontKafka(cfg)
	logrus.AddHook(logstash.GetLogrusHook())
}

const (
	_ FrontStatus = iota
	FrontStatusKafkaConnecting
	FrontStatusKafkaConnected
	FrontStatusKafkaReconnecting
	FrontStatusKafkaDisconnected
	FrontStatusStopping
	FrontStatusStopped
)

type FrontKafka struct {
	producer     sarama.AsyncProducer
	logChan      chan *logrus.Entry
	logFile      *os.File
	fileMu       sync.Mutex
	config       *LogstashFrontKafkaConfig
	status       FrontStatus
	statusChange chan FrontStatus
	ctx          context.Context
	cancel       context.CancelFunc
	sync.Mutex
}

type LogstashFrontKafkaConfig struct {
	//应用名,用于创建ES索引
	AppName string
	//连接Kafka失败时信息储存位置
	FileLogPath string
	//trace字段预设值
	TracePrefix string
	ZKHosts     []string
}

func NewLogstashFrontKafkaConfig() *LogstashFrontKafkaConfig {
	return &LogstashFrontKafkaConfig{}
}

func NewLogstashFrontKafka(config *LogstashFrontKafkaConfig) *FrontKafka {
	r := &FrontKafka{
		statusChange: make(chan FrontStatus, 1),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	r.config = config
	if r.config.FileLogPath == "" {
		r.config.FileLogPath = r.config.AppName + ".log"
	}

	r.openLogFile()

	r.logChan = make(chan *logrus.Entry, 300)

	go r.eventLoop()
	go r.writeWorkFile()
	go r.writeWorkKafka()

	r.statusChange <- FrontStatusKafkaDisconnected
	return r
}

func (l *FrontKafka) openLogFile() {
	var err error

	l.logFile, err = os.OpenFile(l.config.FileLogPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(err)
	}
}

func (l *FrontKafka) eventLoop() {
	for {
		if l.status >= FrontStatusStopping {
			return
		}

		l.eventHandle()
	}
}
func (l *FrontKafka) eventHandle() {
	defer func() {
		if p := recover(); p != nil {
			logrus.Error(fmt.Sprintf(logPrefix+"%s", p))
		}
	}()

	select {
	case <-l.ctx.Done():
		return

	case status, ok := <-l.statusChange:
		if ok {
			switch status {
			case FrontStatusKafkaDisconnected:
				l.onDisconnected()
			case FrontStatusKafkaConnected:
				l.status = FrontStatusKafkaConnected
				logrus.Info(logPrefix + "kafka connect success")
				go l.onConnected()
			}
		}
	}
}
func (l *FrontKafka) getFileLogAndClean() (data []byte) {
	l.fileMu.Lock()
	defer l.fileMu.Unlock()

	_, err := l.logFile.Seek(0, 0)
	if err != nil {
		panic(err)
	}
	data, err = ioutil.ReadAll(l.logFile)
	if err != nil {
		panic(err)
	}
	l.logFile.Close()

	err = os.Truncate(l.config.FileLogPath, 0)
	if err != nil {
		panic(err)
	}
	l.openLogFile()
	return
}

func (l *FrontKafka) writeBackToFile(buf *bytes.Buffer) {
	l.fileMu.Lock()
	defer l.fileMu.Unlock()

	_, err := l.logFile.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}
}

func (l *FrontKafka) onConnected() {
	data := l.getFileLogAndClean()
	buf := bytes.NewBuffer(data)
	for {
		line, err := buf.ReadBytes('\n')

		if err != nil && len(line) == 0 {
			return
		}

		if l.status != FrontStatusKafkaConnected {
			l.writeBackToFile(buf)
			return
		}
		var logStruct struct {
			Timestamp string `json:"@timestamp"`
			Level     string
			Message   string
		}
		err = json.Unmarshal(line, &logStruct)
		if err != nil {
			logrus.Error(logPrefix + err.Error())
			continue
		}
		logLine := map[string]interface{}{}
		err = json.Unmarshal(line, &logLine)
		if err != nil {
			logrus.Error(logPrefix + err.Error())
			continue
		}
		delete(logLine, "message")
		delete(logLine, "@timestamp")
		delete(logLine, "level")

		level, _ := logrus.ParseLevel(logStruct.Level)
		logTime, _ := time.Parse(timestampFormat, logStruct.Timestamp)
		logrus.WithFields(logLine).WithTime(logTime).Log(level, logStruct.Message)
	}
}
func (l *FrontKafka) writeEntryToKafka(entry *logrus.Entry) {
	logBytes, err := l.kafkaFormatter().Format(entry)
	if err != nil {
		logrus.Panicf(logPrefix+"format entry error\n%s", err)
	}

	//构建发送的消息，
	msg := &sarama.ProducerMessage{
		Value:    sarama.ByteEncoder(logBytes),
		Topic:    topic,
		Metadata: entry,
	}

	select {
	case l.producer.Input() <- msg:
	case errMsg := <-l.producer.Errors():
		l.statusChange <- FrontStatusKafkaDisconnected
		l.logChan <- errMsg.Msg.Metadata.(*logrus.Entry)
		logrus.Error(fmt.Sprintf("%s\nfail msg: %s", errMsg.Err, errMsg.Msg.Value.(sarama.ByteEncoder)))
	}
}

func (l *FrontKafka) writeWorkKafka() {
	for {
		if l.status == FrontStatusKafkaConnected {
			select {
			case <-l.ctx.Done():
				return
			case entry, ok := <-l.logChan:
				if ok {
					l.writeEntryToKafka(entry)
				}
			}
		} else {
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (l *FrontKafka) writeWorkFile() {
	for {
		if l.status != FrontStatusKafkaConnected {
			select {
			case <-l.ctx.Done():
				return
			case entry, ok := <-l.logChan:
				if ok {
					if l.status == FrontStatusKafkaConnected {
						l.logChan <- entry
						continue
					}

					l.writeEntryToFile(entry)
				}
			}
		} else {
			time.Sleep(time.Millisecond * 100)
		}
	}
}
func (l *FrontKafka) writeEntryToFile(entry *logrus.Entry) {
	logBytes, err := l.kafkaFormatter().Format(entry)
	if err != nil {
		logrus.Panicf(logPrefix+"format entry error\n%s", err)
	}

	l.fileMu.Lock()
	defer l.fileMu.Unlock()

	_, err = fmt.Fprint(l.logFile, string(logBytes))
	if err != nil {
		fmt.Println(logPrefix + "[error]write log to file: " + err.Error())
	}
}

func (l *FrontKafka) onDisconnected() {
	lastStatus := l.status
	if l.status == FrontStatusKafkaConnecting {
		return
	}
	l.status = FrontStatusKafkaConnecting
	success := false
	defer func() {
		if success {
			l.statusChange <- FrontStatusKafkaConnected
		} else {
			l.status = FrontStatusKafkaReconnecting
			time.Sleep(time.Millisecond * 100)
			l.statusChange <- FrontStatusKafkaDisconnected
		}
	}()

	defer func() {
		if p := recover(); p != nil {
			if lastStatus != FrontStatusKafkaReconnecting {
				logrus.Error(logPrefix + fmt.Sprintf("connect fail:\n\t%s", p))
			}
		}
	}()

	zkHosts := l.getBreakerHosts()

	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.DialTimeout = time.Second * 5
	saramaConfig.Net.ReadTimeout = time.Second * 5
	saramaConfig.Net.WriteTimeout = time.Second * 5
	saramaConfig.Producer.Timeout = time.Second * 5

	saramaConfig.Producer.Return.Errors = true
	// 随机的分区类型：返回一个分区器，该分区器每次选择一个随机分区
	saramaConfig.Producer.Partitioner = sarama.NewRandomPartitioner

	producer, err := sarama.NewAsyncProducer(zkHosts, saramaConfig)
	if err != nil {
		return
	}

	l.producer = producer
	l.status = FrontStatusKafkaConnected

	success = true
	return
}

func (l *FrontKafka) getBreakerHosts() (hosts []string) {

	conn, _, err := zk.Connect(l.config.ZKHosts, time.Second*5)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// get
	children, _, err := conn.Children("/brokers/ids")
	if err != nil {
		panic(err)
	}
	var data []byte
	for _, child := range children {
		// get
		data, _, err = conn.Get(path.Join("/brokers/ids", child))
		if err != nil {
			logrus.Error(err)
			continue
		}

		var broker struct {
			Host string
			Port int
		}

		err = json.Unmarshal(data, &broker)
		if err != nil {
			logrus.Error(err)
			continue
		}

		hosts = append(hosts, fmt.Sprintf("%s:%d", broker.Host, broker.Port))
	}

	return
}

func (l *FrontKafka) kafkaFormatter() *logrus.JSONFormatter {
	return &logrus.JSONFormatter{
		TimestampFormat: timestampFormat,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "@timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
			logrus.FieldKeyFunc:  "caller",
		},
	}
}

func (l *FrontKafka) GetLogrusHook() Hook {
	return Hook{l}
}

type Hook struct {
	core *FrontKafka
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
	return logrus.AllLevels
}
