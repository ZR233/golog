/*
@Time : 2019-10-12 10:29
@Author : zr
*/
package golog

type QueueProducer interface {
	Receive(logRecord *Log) error
}
type QueueConsumer interface {
	Get() (logRecord *Log, err error)
}
