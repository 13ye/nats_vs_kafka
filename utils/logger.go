package utils

import (
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

var (
	logDirPath string = "./logs"
	Logger     *logrus.Logger
)

func init() {
	os.MkdirAll(logDirPath, os.ModeDir)
	file, err := os.OpenFile(logDirPath+"/cdn_diff.log", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		panic("Program Init Failed! Cannot open Log File!")
	}
	mout := io.MultiWriter(os.Stdout, file)
	Logger = logrus.New()
	Logger.SetOutput(mout)
	customFormatter := new(logrus.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02+15:04:05.999999"
	logrus.SetFormatter(customFormatter)
	Logger.SetFormatter(customFormatter)
	//Logger.SetLevel(logrus.DebugLevel)
}
