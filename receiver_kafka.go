/*
@Time : 2019-10-11 11:20
@Author : zr
*/
package golog

import (
	"encoding/json"
	"github.com/Shopify/sarama"
)

type ReceiverKafka struct {
	producer sarama.SyncProducer
	topic    string
}

func NewReceiverKafka(addrs []string, topic string) *ReceiverKafka {
	r := &ReceiverKafka{}
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
func (l *ReceiverKafka) Receive(logRecord *Log) (err error) {
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
