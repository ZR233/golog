/*
@Time : 2019-10-12 10:26
@Author : zr
*/
package golog

import "time"
import log2 "log"

// 日志处理类
type Processor struct {
	queueConsumer QueueConsumer
	writers       []Writer
}

func NewProcessor(consumer QueueConsumer) *Processor {
	p := &Processor{}
	p.queueConsumer = consumer
	return p
}

func (p *Processor) AddWriter(writer Writer) {
	p.writers = append(p.writers, writer)
}

func (p *Processor) Run() {
	for {
		log, err := p.queueConsumer.Get()
		if err != nil {
			log2.Println(err)
			time.Sleep(time.Second)
			continue
		}

		for _, v := range p.writers {
			err = v.Write(log)
			if err != nil {
				log2.Println(err)
			}
		}
	}
}
