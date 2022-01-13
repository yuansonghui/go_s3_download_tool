package main

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"s3_download_tool/src/config_opt"
	"s3_download_tool/src/log"
	"s3_download_tool/src/s3_opt"
	"s3_download_tool/src/util"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	WORK_DIR                string
	COMMON_CONF             map[string]string
	URILIST                 chan string
	mutex                   sync.Mutex
	WORKER_NUM              int64 = 0
	LIST_COMPLETED          bool  = false
	DOWNLOAD_BASE_PATH      string
	DOWNLOAD_RECODE_SECTION string
)

// 获取项目路径，初始化日志对象
func Init() {
	WORK_DIR, _ = os.Getwd()
	log.InitLogger(WORK_DIR)
	log.Log.Info("start...")
}

// 初始化common 配置信息
func InitCommonConfig() {
	common_conf, err := config_opt.GetCommonConfig(WORK_DIR)
	if err != nil {
		log.Log.Error(err.Error())
		os.Exit(1)
	}
	DOWNLOAD_RECODE_SECTION = common_conf["from_date"]
	if _, ok := common_conf["from_date"]; ok {
		from_date_gmt, err := util.TimeStrToGMT(common_conf["from_date"])
		if err != nil {
			log.Log.Error("InitCommonConfig failed;" + err.Error())
			os.Exit(-1)
		}
		common_conf["from_date"] = from_date_gmt
	} else {
		log.Log.Error("Key from_date not in config")
		os.Exit(-1)
	}

	if _, ok := common_conf["until_date"]; ok {
		until_date_gmt, err := util.TimeStrToGMT(common_conf["until_date"])
		if err != nil {
			log.Log.Error("InitCommonConfig failed;" + err.Error())
			os.Exit(-1)
		}
		common_conf["until_date"] = until_date_gmt
	} else {
		log.Log.Error("Key until_date not in config")
		os.Exit(-1)
	}
	now := time.Now()
	folder_name := now.Format("20060102_150405")
	var pathList = []string{WORK_DIR, folder_name}
	common_conf["download_path"] = config_opt.GetPathForOsType(pathList)
	COMMON_CONF = common_conf
}

// 下载对象
func DoloadObject(s3_session *s3.S3, bucket string, obj_name string, worker int) error {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(obj_name),
	}
	resp, err := s3_session.GetObject(params)
	if err != nil {
		log.Log.Error("DoloadObject", obj_name, "failed, err is ", err.Error())
		return err
	}
	var pathList = []string{WORK_DIR, "mos_data", bucket, obj_name}
	dst_name := config_opt.GetPathForOsType(pathList)

	paths, _ := filepath.Split(dst_name)
	if !util.CreateDir(paths) {
		log.Log.Error("CreateDir", paths, "failed")
		return err
	}
	wt, err := os.OpenFile(dst_name, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		if strings.Contains(err.Error(), "is a directory") {
			return nil
		}
		log.Log.Error("OpenFile", dst_name, "failed, err:", err.Error())
		return err
	}
	defer wt.Close()
	num, err := io.Copy(wt, resp.Body)
	if err != nil {
		log.Log.Error("IoCopy", dst_name, "failed, err:", err.Error())
	}
	log.Log.Info("Worker: ", worker, ", download successfully ", dst_name, " num: ", num)
	return nil
}

// 下载对象总入口，使用goroutine 执行，每个线程读取URILIST channel 数据， 每个对象失败重试3次
func AsyncDownloadWorker(worker int, s3_session *s3.S3, bucketDownloadInfo map[string]map[string]int) {
	log.Log.Info("Worker: ", worker, "  start...")
	for !LIST_COMPLETED {
		for datastr := range URILIST {
			bucket, key := util.GetDataBySign(datastr)
			isSucc := false
			for i := 0; i < 3; i++ {
				result := DoloadObject(s3_session, bucket, key, worker)
				if result == nil {
					isSucc = true
					break
				}
			}
			if !isSucc {
				log.Log.Error("Download failed: ", bucket, " ", key)
			}
			mutex.Lock()
			bucketDownloadInfo[bucket]["completed"] += 1
			mutex.Unlock()
		}
		time.Sleep(time.Second * 10)
	}
	mutex.Lock()
	WORKER_NUM -= 1
	mutex.Unlock()
	log.Log.Info("Worker: ", worker, " urllist is empty, exit...")
}

func ListAndDownload(s3_session *s3.S3, bucket string, max_keys int64, bucketDownloadInfo map[string]map[string]int) error {
	log.Log.Info("start to ListAndDownload ", bucket)
	truncatedListing := true
	nextMarker := aws.String("")
	for truncatedListing {
		params := &s3.ListObjectsInput{
			Bucket:  aws.String(bucket),
			Marker:  nextMarker,
			MaxKeys: aws.Int64(max_keys),
		}
		req, resp := s3_session.ListObjectsRequest(params)
		req.HTTPRequest.Header.Add("from_date", COMMON_CONF["from_date"])
		req.HTTPRequest.Header.Add("until_date", COMMON_CONF["until_date"])

		err := req.Send()
		if err != nil {
			log.Log.Error("ListObjectsRequest failed", err.Error())
			continue
		}
		for _, item := range resp.Contents {
			bucketDownloadInfo[bucket]["all"] += 1
			unique_str := util.BuildStringBySign(bucket, *item.Key)
			URILIST <- unique_str
		}
		truncatedListing = *resp.IsTruncated
		nextMarker = resp.NextMarker
	}
	log.Log.Info(bucket, " has listobject completed, wait download...")
	return nil
}

func IsBucketDownloadCompleted(configObj map[string]string, bucketName string) bool {
	if value, ok := configObj[bucketName]; ok {
		if value == "completed" {
			return true
		}
		return false
	} else {
		return false
	}
}

func ServiceRun() {
	// 获取common 配置信息
	InitCommonConfig()
	// 获取单次列举对象最大个数，默认1000
	max_keys, _ := strconv.ParseInt(COMMON_CONF["max_keys"], 10, 64)
	if max_keys <= 0 {
		max_keys = int64(1000)
	}
	// 初始化URILIST， 保存等待下载的对象路径
	URILIST = make(chan string, max_keys)
	// 获取 s3 client 相关配置
	ip, port, ak, sk := config_opt.GetS3ClientInfo(WORK_DIR)
	// 初始化s3 session
	s3_cls := s3_opt.InitConfig(ip, port, ak, sk)
	s3_session := s3_cls.ConnectS3()
	// 获取所有桶对象
	bucket_info, err := s3_session.ListBuckets(nil)
	if err != nil {
		log.Log.Error("ListBuckets failed" + err.Error())
	}

	maxWorker, err := strconv.ParseInt(COMMON_CONF["download_max_num"], 10, 64)
	if err != nil {
		log.Log.Error("Get maxWorker failed" + err.Error())
		maxWorker = 50
	}
	// 记录每个bucket总共需要下载的对象数以及已经下载的对象个数
	bucketDownloadInfo := make(map[string]map[string]int)

	for i := 1; i <= int(maxWorker); i++ {
		go AsyncDownloadWorker(i, s3_session, bucketDownloadInfo)
		WORKER_NUM += 1
	}
	// 程序启动时获取下载记录
	downloadRecode := make(map[string]string)
	downloadRecode = config_opt.GetDownloadRecode(WORK_DIR, DOWNLOAD_RECODE_SECTION)

	// 遍历每个桶，将对象写入到 URILIST 管道中，URILIST管道设置长度，会阻塞住for 循环；
	for _, b := range bucket_info.Buckets {
		bucketName := aws.StringValue(b.Name)
		// 对比下载记录，跳过下载完成的bucket
		if IsBucketDownloadCompleted(downloadRecode, bucketName) {
			log.Log.Info("check bucket: ", bucketName, " has download completed, will ignore")
			continue
		}
		bucketDownloadInfo[bucketName] = make(map[string]int)
		bucketDownloadInfo[bucketName]["all"] = 0
		bucketDownloadInfo[bucketName]["completed"] = 0
		config_opt.WriteDownloadRecode(WORK_DIR, DOWNLOAD_RECODE_SECTION, bucketName, "wait")
		ListAndDownload(s3_session, bucketName, max_keys, bucketDownloadInfo)
	}
	LIST_COMPLETED = true
	log.Log.Info("Has list all object, will close channel URILIST")
	close(URILIST)

	for len(URILIST) > 0 {
		for bucketName, value := range bucketDownloadInfo {
			log.Log.Info("Is downloading, Bucket: ", bucketName, "; all num is: ", value["all"], "; completed: ", value["completed"])
		}
		time.Sleep(time.Second * 20)
	}
	for WORKER_NUM > 0 {
		for bucketName, value := range bucketDownloadInfo {
			log.Log.Info("Is downloading, Bucket: ", bucketName, "; all num is: ", value["all"], "; completed: ", value["completed"], " worker num: ", WORKER_NUM)
			if value["all"] == value["completed"] {
				config_opt.WriteDownloadRecode(WORK_DIR, DOWNLOAD_RECODE_SECTION, bucketName, "completed")
			}
		}
		time.Sleep(time.Second * 10)
	}
	log.Log.Info("Download completed!")
}

func main() {
	// work_dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	runtime.GOMAXPROCS(4)
	Init()
	ServiceRun()
}
