package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/JokeCiCi/logagent/config"
	"github.com/JokeCiCi/logagent/kafka"
	"github.com/JokeCiCi/logagent/taillog"
)

var wg sync.WaitGroup

// 列出目录下文件
// 获取文件ino判断
// 1.正在监控，跳过
// 2.未监控，NewTailTask
// -> 1.查询offset文件
// -> 2.有表示之前监控过，读取尾行offset
// -> 3.没有表示新文件，创建offset
func main() {
	// 初始化配置信息
	err := config.Init()
	if err != nil {
		fmt.Printf("config init failed, err:%v \n", err)
		os.Exit(1)
	}

	// 初始化kafka
	err = kafka.Init([]string{config.AppCfg.KafkaConf.Address}, config.AppCfg.KafkaConf.ChanMaxSize, &wg)
	if err != nil {
		fmt.Printf("kafka init failed, err:%v \n", err)
		os.Exit(1)
	}

	// 初始化tailmgr
	err = taillog.Init(config.AppCfg.LogDir, config.AppCfg.OffsetDir, config.AppCfg.KafkaConf.Topic, config.AppCfg.IsLogging, &wg)
	if err != nil {
		fmt.Printf("taillog init failed, err:%v \n", err)
		os.Exit(1)
	}
	wg.Wait()
	fmt.Println("closed")
}
