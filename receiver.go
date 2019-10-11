/*
@Time : 2019-10-11 11:02
@Author : zr
*/
package golog

type Receiver interface {
	Receive(logRecord *Log) error
}
