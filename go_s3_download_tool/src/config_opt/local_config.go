package config_opt

import (
	"os"
	"runtime"
	"strconv"

	"github.com/Unknwon/goconfig"
)

var OsType = runtime.GOOS

type ConfigOption interface {
	Get() LocalConfig
}

type LocalConfig struct {
	IP   string
	Port string
	Sk   string
	Ak   string
}

func (conf LocalConfig) Get(wd_path string) *LocalConfig {
	// wd_path, _ := filepath.Abs(filepath.Dir(os.Args[0]))

	var pathList = []string{wd_path, "config.ini"}
	configPath := GetPathForOsType(pathList)
	config_obj, err := goconfig.LoadConfigFile(configPath)
	if err != nil {
		println("LoadConfigFile failed" + err.Error())
		return &LocalConfig{IP: "", Port: "", Sk: "", Ak: ""}
	}
	ip, _ := config_obj.GetValue("s3_config", "ip")
	port, _ := config_obj.GetValue("s3_config", "port")
	ak, _ := config_obj.GetValue("s3_config", "ak")
	sk, _ := config_obj.GetValue("s3_config", "sk")
	return &LocalConfig{IP: ip, Port: port, Ak: ak, Sk: sk}
}

func LoadS3Config() *LocalConfig {
	loc_cfg := LocalConfig{}
	return &loc_cfg
}

func GetS3ClientInfo(wd_path string) (ip string, port string, ak string, sk string) {
	config_obj := LoadS3Config().Get(wd_path)
	ip, port, ak, sk = config_obj.IP, config_obj.Port, config_obj.Ak, config_obj.Sk
	return
}

func GetLogConfig(wd_path string) (level string, logsize int64) {
	level, logsize = "info", 104857600
	var pathList = []string{wd_path, "config.ini"}
	configPath := GetPathForOsType(pathList)
	config_obj, err := goconfig.LoadConfigFile(configPath)
	if err != nil {
		println("LoadConfigFile failed" + err.Error())
		return
	}
	level, _ = config_obj.GetValue("log", "level")
	size, _ := config_obj.GetValue("log", "logsize")
	logsize, _ = strconv.ParseInt(size, 10, 64)
	return
}

func GetCommonConfig(wd_path string) (map[string]string, error) {
	var pathList = []string{wd_path, "config.ini"}
	configPath := GetPathForOsType(pathList)
	config_obj, err := goconfig.LoadConfigFile(configPath)
	if err != nil {
		println("LoadConfigFile failed" + err.Error())
		return map[string]string{}, err
	}
	conf_dict, _ := config_obj.GetSection("common")
	return conf_dict, nil
}

func GetPathForOsType(pathList []string) string {
	resPath := pathList[0]
	if OsType == "windows" {
		for _, ipath := range pathList[1:] {
			resPath = resPath + "\\" + ipath
		}
	} else {
		for _, ipath := range pathList[1:] {
			resPath = resPath + "/" + ipath
		}
	}
	return resPath
}

func WriteDownloadRecode(wd_path string, fromDate string, completedBucket string, status string) {
	var pathList = []string{wd_path, "download_recode.ini"}
	configName := GetPathForOsType(pathList)
	// configName := "download_recode.ini"
	InitDownloadRecode(configName)
	config_obj, _ := goconfig.LoadConfigFile(configName)
	config_obj.SetValue(fromDate, completedBucket, status)
	goconfig.SaveConfigFile(config_obj, configName)
}

func InitDownloadRecode(cfgPath string) {
	_, err := os.Stat(cfgPath)
	if os.IsNotExist(err) {
		file, err := os.Create(cfgPath)
		if err == nil {
			file.Close()
		}
	}
}

func GetDownloadRecode(wd_path string, fromDate string) map[string]string {
	var pathList = []string{wd_path, "download_recode.ini"}
	configName := GetPathForOsType(pathList)
	InitDownloadRecode(configName)
	config_obj, _ := goconfig.LoadConfigFile(configName)
	value, _ := config_obj.GetSection(fromDate)
	return value
}
