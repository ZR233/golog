/*
@Time : 2019-10-12 10:18
@Author : zr
*/
package golog

import (
	"fmt"
	"github.com/ZR233/goutils"
	"github.com/jinzhu/gorm"
	log2 "log"
	"strings"
	"time"
)

type WriterSQL struct {
	db           *gorm.DB
	logChan      chan *Log
	logTableName string
	logAliveTime time.Duration
}

// Open initialize a new db connection, need to import driver first, e.g:
//
//     import _ "github.com/go-sql-driver/mysql"
//     func main() {
//       db, err := gorm.Open("mysql", "user:password@/dbname?charset=utf8&parseTime=True&loc=Local")
//     }
// GORM has wrapped some drivers, for easier to remember driver's import path, so you could import the mysql driver with
//    import _ "github.com/jinzhu/gorm/dialects/mysql"
//    // import _ "github.com/jinzhu/gorm/dialects/postgres"
//    // import _ "github.com/jinzhu/gorm/dialects/sqlite"
//    // import _ "github.com/jinzhu/gorm/dialects/mssql"
func NewWriterSQL(dialect, connStr, logTableName string) *WriterSQL {
	w := &WriterSQL{}

	db, err := gorm.Open(dialect, connStr)
	if err != nil {
		panic(err)
	}
	if err := db.AutoMigrate(&Log{tableName: logTableName}).Error; err != nil {
		panic(err)
	}
	w.logAliveTime = time.Hour * 24 * 365 * 20
	w.db = db
	w.logChan = make(chan *Log, 1)
	w.logTableName = logTableName
	go w.serve()
	go func() {
		for {
			w.clearLoop()
			time.Sleep(time.Hour)
		}
	}()
	return w
}
func (w *WriterSQL) SetLogAliveTime(duration time.Duration) {
	w.logAliveTime = duration
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
		args = append(args, v.Time, v.Trace, v.Level, v.Code, v.OptUserId, goutils.StringChineseMaxLen(v.Msg, 2000), v.ExecTime)
	}
	sqlStr = strings.TrimRight(sqlStr, ",")
	beginTime := time.Now()

	db = db.Exec(sqlStr, args...)

	endTime := time.Now()

	err := db.Error
	if err != nil {
		panic(err)
	}
	insertCount := db.RowsAffected
	execTime := endTime.Sub(beginTime)
	log2.Println(fmt.Sprintf("%d records,insert %d to db, use ", recordCount, insertCount), execTime)

}
func (w *WriterSQL) clearLoop() {
	deleteTime := time.Now().Add(-w.logAliveTime)

	db := w.db.Where("time < ?", deleteTime).Delete(&Log{tableName: w.logTableName})
	if db.Error != nil {
		log2.Println(db.Error)
	} else {
		log2.Println(fmt.Sprintf("clear %d rows", db.RowsAffected))
	}
}
