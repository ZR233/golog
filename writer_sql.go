/*
@Time : 2019-10-12 10:18
@Author : zr
*/
package golog

import (
	"database/sql"
	"fmt"
	log2 "log"
	"strings"
	"time"
)

type WriterSQL struct {
	db           *sql.DB
	logChan      chan *Log
	logTableName string
}

func NewWriterSQL(db *sql.DB, logTableName string) *WriterSQL {
	w := &WriterSQL{}
	w.db = db
	w.logChan = make(chan *Log, 1)
	w.logTableName = logTableName
	go w.serve()
	return w
}

func (w *WriterSQL) Write(log *Log) error {
	w.logChan <- log
	return nil
}

func (w *WriterSQL) serve() {
	for {
		w.saveLoop()
	}
}

func (w *WriterSQL) saveLoop() {
	defer func() {
		if err := recover(); err != nil {
			log2.Println(err)
		}
	}()

	var logs []*Log
	timer := time.NewTimer(time.Second * 2)

loop:
	for i := 0; i < 1000; i++ {
		select {
		case log := <-w.logChan:
			logs = append(logs, log)
		case <-timer.C:
			break loop
		}
	}
	recordCount := len(logs)
	if recordCount == 0 {
		return
	}

	sqlStr := `INSERT INTO ` + w.logTableName + `(time,trace,level,code,opt_user_id,msg,exec_time)VALUES`

	var args []interface{}
	db := w.db
	for _, v := range logs {
		sqlStr += `(?,?,?,?,?,?,?),`
		args = append(args, v.Time, v.Trace, v.Level, v.Code, v.OptUserId, v.Msg, v.ExecTime)
	}
	sqlStr = strings.TrimRight(sqlStr, ",")

	stmt, err := db.Prepare(sqlStr)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	r, err := stmt.Exec(args...)
	if err != nil {
		panic(err)
	}
	insertCount, _ := r.RowsAffected()

	log2.Println(fmt.Sprintf("%d records,insert %d to db, ", recordCount, insertCount))

}
