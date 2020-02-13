package taillog

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/JokeCiCi/logagent/config"
	"github.com/JokeCiCi/logagent/kafka"
)

// TaillogMgr 监控管理
type TaillogMgr struct {
	tasks  map[string]*TailTask
	ctx    context.Context
	cancel context.CancelFunc
}

var taillogMgr *TaillogMgr

// Init 初始化 ...
func Init(logDir, offsetDir, topic string, isLogging bool, wg *sync.WaitGroup) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	taillogMgr = &TaillogMgr{
		tasks:  make(map[string]*TailTask, 100),
		ctx:    ctx,
		cancel: cancel,
	}
	err = taillogMgr.init(logDir, offsetDir, topic, isLogging, wg)
	if err != nil {
		fmt.Printf("taillogMgr init failed, err:%v \n", err)
		return
	}
	go taillogMgr.run(wg)
	return
}

// GetLastLine 获取文件的最后一行内容
func GetLastLine(filePath string) (lastLine string, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("open file failed, err:%v \n", err)
		return
	}
	r := bufio.NewReader(f)
	for {
		var curLine string
		curLine, err = r.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("read string failed, err:%v \n", err)
			return
		}
		curLine = strings.TrimSpace(curLine)
		if len(curLine) > 0 {
			lastLine = curLine
		}
	}
	return
}

func (t *TaillogMgr) init(logDir, offsetDir, topic string, isLogging bool, wg *sync.WaitGroup) (err error) {
	if !isLogging {
		fmt.Println("isLogging set false close all task")
		t.Shutdown()
	}

	// 创建目录
	if _, err = os.Stat(logDir); os.IsNotExist(err) {
		if err = os.Mkdir(logDir, os.ModePerm); err != nil {
			fmt.Printf("mkdir %s failed, err:%v \n", logDir, err)
			return
		}
	}
	if _, err = os.Stat(offsetDir); os.IsNotExist(err) {
		if err = os.Mkdir(offsetDir, os.ModePerm); err != nil {
			fmt.Printf("mkdir %s failed, err:%v \n", offsetDir, err)
			return
		}
	}

	// 读取日志文件信息
	logFileInfos, err := ioutil.ReadDir(logDir)
	if err != nil {
		fmt.Printf("read dir failed, err:%v \n", err)
		return
	}

	for _, logFileInfo := range logFileInfos {
		logFileName := logFileInfo.Name()                  // 日志文件名
		inodeID := logFileInfo.Sys().(*syscall.Stat_t).Ino // 日志文件的inode ID
		offsetFileName := strconv.FormatUint(inodeID, 10)  // 日志偏移文件名
		logPath := path.Join(logDir, logFileInfo.Name())   // 日志文件路径
		offsetPath := path.Join(offsetDir, offsetFileName) // 偏移文件路径

		lastLine, _ := GetLastLine(path.Join(config.AppCfg.OffsetDir, offsetFileName))
		fmt.Printf("日志文件名:%v 日志文件大小:%d 日志偏移文件名:%v 日志偏移文件值%v \n", logFileName, logFileInfo.Size(), offsetFileName, lastLine)
		// 过滤：目录、标记end的日志、正在监控的日志
		if logFileInfo.IsDir() {
			fmt.Printf("%s is dir \n", logFileName)
			continue
		}
		if strings.HasSuffix(logFileName, "end") {
			continue
		}
		if taillogMgr.isRunningTask(offsetFileName) {
			continue
		}

		// 创建任务
		if _, err := os.Stat(offsetPath); os.IsNotExist(err) { // 没有偏移文件
			err = taillogMgr.addTask(logPath, offsetPath, topic, offsetFileName, 0, wg)
			if err != nil {
				fmt.Printf("tailMgr add task failed, err:%v \n", err)
				continue
			}
		} else { // 有偏移文件
			offsetStr, err := GetLastLine(offsetPath)
			if err != nil && err != io.EOF {
				fmt.Printf("get last line failed, err:%v \n", err)
				continue
			}
			if offsetStr == "" {
				offsetStr = "0"
			}
			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				fmt.Printf("parse offset int failed, err:%v \n", err)
				continue
			}

			if offset == logFileInfo.Size() { // 偏移值和文件大小一致
				os.Rename(logPath, fmt.Sprintf("%s.end", logPath))
				os.Rename(offsetPath, fmt.Sprintf("%s.end", offsetPath))
			} else { // 且偏移值和文件不大小一致，即之前进程挂了未读完文件
				err = taillogMgr.addTask(logPath, offsetPath, topic, offsetFileName, offset, wg)
				if err != nil {
					fmt.Printf("tailMgr add task failed, err:%v \n", err)
					continue
				}
			}
		}
	}
	return
}

// putTask 增加task
func (t *TaillogMgr) addTask(logPath, offsetPath, topic, offsetFileName string, offset int64, wg *sync.WaitGroup) (err error) {
	tt, err := NewTailTask(logPath, offsetPath, config.AppCfg.KafkaConf.Topic, offset, wg)
	if err != nil {
		fmt.Printf("new tail task failed, err:%v \n", err)
		return
	}
	t.tasks[offsetFileName] = tt
	return
}

// deleteTask 删除task
func (t *TaillogMgr) deleteTask(offsetFileName string) (err error) {
	tt, ok := t.tasks[offsetFileName]
	if !ok {
		err = fmt.Errorf("TailTask not found, offsetFileName: %v", offsetFileName)
	}
	tt.cancel()
	delete(t.tasks, offsetFileName)
	return
}

func (t *TaillogMgr) isRunningTask(inodeIDStr string) (ok bool) {
	_, ok = t.tasks[inodeIDStr]
	return
}

// run 定时加载配置创建task
func (t *TaillogMgr) run(wg *sync.WaitGroup) {
	for {
		select {
		case <-time.Tick(time.Second * 30):
			fmt.Println("start load", time.Now().Format("2006-01-02 15:04:05"))
			config.Init()
			t.init(config.AppCfg.LogDir, config.AppCfg.OffsetDir, config.AppCfg.KafkaConf.Topic, config.AppCfg.IsLogging, wg)
		case <-t.ctx.Done():
			fmt.Println("goroutine TaillogMgr done")
			return
		}
	}
}

// Shutdown 关闭所有的task
func (t *TaillogMgr) Shutdown() {
	for k, _ := range t.tasks {
		t.deleteTask(k)
	}
	t.cancel()
	kafka.CloseChan()
}
