package log

import (
	"os"
	"path/filepath"
	"s3_download_tool/src/config_opt"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	log "github.com/sirupsen/logrus"
)

var Log = log.NewEntry(&log.Logger{Out: os.Stdout, Formatter: &log.JSONFormatter{}, Level: log.InfoLevel})

func InitLogger(work_dir string) {
	log_level, logsize := config_opt.GetLogConfig(work_dir)
	var pathList = []string{work_dir, "logs", "message.log"}
	file := config_opt.GetPathForOsType(pathList)
	log_rotate, _ := rotatelogs.New(
		file+".%Y%m%d%H%M",
		rotatelogs.WithRotationSize(logsize),
	)
	paths, _ := filepath.Split(file)
	os.MkdirAll(paths, 0755)
	// logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	// if err != nil {
	// 	panic(err)
	// }
	level, err := log.ParseLevel(log_level)
	if err != nil {
		log.Error("ParseLevel failed, error:" + err.Error())
	}
	log.SetOutput(log_rotate)
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(level)
	log.SetReportCaller(true)
	Log = log.WithFields(log.Fields{})
}
