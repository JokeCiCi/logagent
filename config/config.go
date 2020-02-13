package config

import (
	"fmt"

	"github.com/go-ini/ini"
)

var (
	AppCfg *AppConf
)

type LogConf struct {
	LogDir    string `ini:"logDir"`
	OffsetDir string `ini:"offsetDir"`
	IsLogging bool   `ini:"isLogging"`
}

type KafkaConf struct {
	Address     string `ini:"address"`
	Topic       string `ini:"topic"`
	ChanMaxSize int    `ini:"chanMaxSize"`
}

type AppConf struct {
	LogConf   `ini:"log"`
	KafkaConf `ini:"kafka"`
}

func Init() (err error) {
	AppCfg = &AppConf{}
	// 加载配置
	err = ini.MapTo(AppCfg, "./config/config.ini")
	if err != nil {
		fmt.Printf("map to failed, err:%v \n", err)
		return
	}
	return
}
