package golog

import (
	"github.com/sirupsen/logrus"
	"testing"
	"time"
)

func TestNewLogstashFrontKafka(t *testing.T) {

	cfg := NewLogstashFrontKafkaConfig()
	cfg.AppName = "test"

	front := NewLogstashFrontKafka(cfg)

	logrus.SetFormatter(&logrus.TextFormatter{})
	logrus.AddHook(front.GetLogrusHook())
	logrus.SetLevel(LogLevelDebug.ToLogrusLevel())
	logrus.Info("test1")
	front.SetKafkaAddrs([]string{"192.168.0.3:9092"})
	i := 2
	go func() {
		for {
			logrus.Infof("test%d", i)
			i++
			time.Sleep(time.Second)
		}
	}()

	time.Sleep(time.Second * 120)

}
