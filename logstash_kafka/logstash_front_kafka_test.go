package logstash_kafka

import (
	"github.com/sirupsen/logrus"
	"testing"
	"time"
)

func TestNewLogstashFrontKafka(t *testing.T) {

	cfg := NewLogstashFrontKafkaConfig()
	cfg.AppName = "test"
	cfg.ZKHosts = []string{"192.168.0.3:2181"}

	front := NewLogstashFrontKafka(cfg)

	logrus.SetFormatter(&logrus.TextFormatter{})
	logrus.AddHook(front.GetLogrusHook())
	logrus.SetLevel(logrus.DebugLevel)
	logrus.Info("test1")

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

func TestLogstashFrontKafka_getBreakerHosts(t *testing.T) {
	cfg := NewLogstashFrontKafkaConfig()
	cfg.ZKHosts = []string{
		"192.168.0.3",
	}
	front := NewLogstashFrontKafka(cfg)
	hosts := front.getBreakerHosts()

	println(hosts)

}
