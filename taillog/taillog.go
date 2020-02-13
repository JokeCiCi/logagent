package taillog

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/JokeCiCi/logagent/kafka"
	"github.com/hpcloud/tail"
)

// TailTask task结构体
type TailTask struct {
	logPath        string
	offsetPath     string
	topic          string
	offsetFileName string
	offset         int64
	instance       *tail.Tail
	offsetFile     *os.File
	ctx            context.Context
	cancel         context.CancelFunc
}

// NewTailTask 初始化tailtask对象
func NewTailTask(logPath, offsetPath, topic string, offset int64, wg *sync.WaitGroup) (tt *TailTask, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	offsetFileName := path.Base(offsetPath)
	tt = &TailTask{
		logPath:        logPath,
		offsetPath:     offsetPath,
		topic:          topic,
		offsetFileName: offsetFileName,
		offset:         offset,
		ctx:            ctx,
		cancel:         cancel,
	}

	err = tt.init()
	if err != nil {
		fmt.Printf("tail task init failed, err:%v \n", err)
		return
	}
	// 启动tailtask
	wg.Add(1)
	go tt.run(wg)

	return
}

// Init 初始化tail对象
func (t *TailTask) init() (err error) {
	// 初始化tail
	instance, err := tail.TailFile(t.logPath,
		tail.Config{
			ReOpen:    false,
			Follow:    false,
			MustExist: true,
			Poll:      true,
			Location:  &tail.SeekInfo{Offset: t.offset, Whence: os.SEEK_SET},
		})
	if err != nil {
		fmt.Printf("tail file failed, err:%v \n", err)
		return
	}
	t.instance = instance

	// 打开offset文件
	var offsetFile *os.File
	offsetFile, err = os.OpenFile(t.offsetPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	fmt.Println("os openfile")
	if err != nil {
		fmt.Printf("open offset file failed, err:%v \n", err)
		return
	}
	t.offsetFile = offsetFile
	return
}

// run 开启日志读取
func (t *TailTask) run(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case line, ok := <-t.instance.Lines:
			if !ok {
				ofsFileInfo, _ := t.offsetFile.Stat()
				fmt.Printf("tail chan Lines closed, ofsFileName:%v logFilePath:%v \n", ofsFileInfo.Name(), t.logPath)
				taillogMgr.deleteTask(t.offsetFileName)
				return
			}
			// 获取offset
			offset, err := t.instance.Tell()
			if err != nil {
				fmt.Printf("get file offset failed, err:%v \n", err)
				taillogMgr.deleteTask(t.offsetFileName)
				return
			}
			kafka.SendToChan(t.topic, "", line.Text, offset, t.offsetFile)
		case <-t.ctx.Done():
			fmt.Println("goroutine TailTask done")
			taillogMgr.deleteTask(t.offsetFileName)
			return
		default:
			time.Sleep(time.Millisecond * 50)
		}
	}
}
