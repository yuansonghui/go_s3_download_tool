package util

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"s3_download_tool/src/log"
	"strings"
	"time"
)

func ExitError(msg string, args ...interface{}) {
	log.Log.Errorf(msg, args)
	os.Exit(1)
}

func TimeStrToGMT(timeStr string) (gmt_time string, err error) {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", timeStr, time.Local)
	if err != nil {
		log.Log.Error(err.Error())
		return "", err
	}
	t_GMT := t.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
	return t_GMT, nil
}

func CreateDir(path_name string) bool {
	_, err := os.Stat(path_name)
	if err == nil {
		return true
	}
	if os.IsExist(err) {
		return true
	}
	err = os.MkdirAll(path_name, 0755)
	return err == nil
}

func GetMd5ByStrings(args ...string) string {
	tmpStr := ""
	for _, value := range args {
		tmpStr = tmpStr + value
	}
	w := md5.New()
	io.WriteString(w, tmpStr)
	md5str := fmt.Sprintf("%x", w.Sum(nil))
	return md5str
}

func BuildStringBySign(bucket string, key string) string {
	res := bucket + "sign::sign" + key
	return res
}

func GetDataBySign(data string) (bucket string, key string) {
	tmp := strings.Split(data, "sign::sign")
	return tmp[0], tmp[1]
}
