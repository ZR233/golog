/*
@Time : 2019-10-11 11:20
@Author : zr
*/
package golog

import (
	"encoding/json"
	"errors"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"os"
	"strings"
	"time"
)

type QueueProducerKafka struct {
	producer sarama.SyncProducer
	topic    string
}

func NewReceiverKafka(addrs []string, topic string) *QueueProducerKafka {
	r := &QueueProducerKafka{}
	config := sarama.NewConfig()

	// 等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 随机的分区类型：返回一个分区器，该分区器每次选择一个随机分区
	config.Producer.Partitioner = sarama.NewManualPartitioner
	// 是否等待成功和失败后的响应
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(addrs, config)
	if err != nil {
		panic(err)
	}
	r.producer = producer
	r.topic = topic
	return r
}
func (l *QueueProducerKafka) Receive(logRecord *Log) (err error) {
	data, err := json.Marshal(logRecord)
	if err != nil {
		return
	}

	//构建发送的消息，
	msg := &sarama.ProducerMessage{
		Partition: int32(0),
		Key:       sarama.StringEncoder("key"),
		Value:     sarama.ByteEncoder(data),
		Topic:     l.topic,
	}

	_, _, err = l.producer.SendMessage(msg)
	return
}

type QueueConsumerKafka struct {
	conn              *cluster.Consumer
	partitionConsumer sarama.PartitionConsumer
	logChan           chan *Log
	signals           chan int
	offsetFile        *os.File
}

func NewQueueConsumerKafka(addrs []string, topic string) (q *QueueConsumerKafka, err error) {
	q = &QueueConsumerKafka{
		logChan: make(chan *Log, 1),
		signals: make(chan int, 1),
	}

	println("----- kafka address -----")
	for _, v := range addrs {
		println(v)
	}
	println("-----      end      -----")
	println("kafka topic: ", topic)
	//f, err := os.OpenFile("log_offset", os.O_RDWR|os.O_CREATE, 0766)
	//if err != nil {
	//	panic(err)
	//}
	//r.offsetFile = f
	//
	//offset := 0
	//data, err := ioutil.ReadAll(f)
	//if err != nil {
	//	panic(err)
	//}
	//n, err := strconv.Atoi(string(data))
	//if err == nil {
	//	offset = n
	//}
	//println(fmt.Sprintf("kafka offset begin: %d", offset))
	//
	//config := sarama.NewConfig()
	//
	//consumer, err := sarama.NewConsumer(addrs, config)
	//if err != nil {
	//	panic(err)
	//}
	//r.conn = consumer
	//
	//pc, err := consumer.ConsumePartition(topic, int32(0), int64(offset))
	//if err != nil {
	//	panic(err)
	//}
	//r.partitionConsumer = pc

	groupID := "group-1"
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = false
	config.Group.Return.Notifications = false
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	q.conn, err = cluster.NewConsumer(addrs, groupID, strings.Split(topic, ","), config)
	if err != nil {
		return
	}

	return
}
func (q *QueueConsumerKafka) Get() (log *Log, err error) {
	log = &Log{}

	select {
	case msg, ok := <-q.conn.Messages():
		if ok {

			err = json.Unmarshal(msg.Value, log)
			if err != nil {
				return
			}
			q.conn.MarkOffset(msg, "")
		} else {
			err = errors.New("kafka can not get msg")
		}
	case <-q.signals:
		break
	}

	return log, nil
}
