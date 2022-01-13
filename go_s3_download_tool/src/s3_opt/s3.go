package s3_opt

import (
	"s3_download_tool/src/log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Mgmt struct {
	Ip   string
	Port string
	Ak   string
	Sk   string
}

func InitConfig(Ip string, Port string, Ak string, Sk string) *S3Mgmt {
	s3cls := S3Mgmt{Ip: Ip, Port: Port, Ak: Ak, Sk: Sk}
	return &s3cls
}

func (s3m S3Mgmt) ConnectS3() *s3.S3 {
	end_point := "http://" + s3m.Ip + ":" + s3m.Port
	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(s3m.Ak, s3m.Sk, ""),
		Endpoint:         aws.String(end_point),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true), //virtual-host style方式，不要修改
	})
	if err != nil {
		log.Log.Error(err)
	}
	svc := s3.New(sess)
	log.Log.Info("ConnectS3: endpoint " + end_point)
	return svc
}
