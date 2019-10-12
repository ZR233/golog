/*
@Time : 2019-10-12 10:20
@Author : zr
*/
package golog

type Writer interface {
	Write(log *Log) error
}
