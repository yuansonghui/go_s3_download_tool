package log

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	logIDs = make(map[uint64]string, 1000)
	locker = sync.RWMutex{}
)

func GoID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func getGoID() uint64 {
	return GoID()
}

type LogIDHook struct {
}

func GetLogId() string {
	locker.RLock()
	defer locker.RUnlock()
	goID := getGoID()
	if logID, ok := logIDs[goID]; ok {
		return logID
	}
	return ""
}

func NewLogIDHook() *LogIDHook {
	return &LogIDHook{}
}

func (hook *LogIDHook) Fire(entry *logrus.Entry) error {
	entry.Data["log_id"] = GetLogId()
	return nil
}

func (hook *LogIDHook) Level() []logrus.Level {
	return logrus.AllLevels
}
