/*
@Time : 2019-11-04 16:16
@Author : zr
*/
package golog

import (
	"github.com/sirupsen/logrus"
	"testing"
)

func TestLogLevel_ToLogrusLevel(t *testing.T) {
	tests := []struct {
		name string
		l    LogLevel
		want logrus.Level
	}{
		{"", LogLevelDebug, logrus.DebugLevel},
		{"", LogLevelError, logrus.ErrorLevel},
		{"", LogLevelPanic, logrus.PanicLevel},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.l.ToLogrusLevel(); got != tt.want {
				t.Errorf("ToLogrusLevel() = %v, want %v", got, tt.want)
			}
		})
	}
}
