package kafka

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	// client      sarama.SyncProducer
	logDataChan chan *LogData
	kt          *KafkaTask
	totalLog    *os.File
)

type KafkaTask struct {
	// client sarama.SyncProducer
	ctx    context.Context
	cancel context.CancelFunc
}

// LogData 日志数据结构体
type LogData struct {
	topic      string
	key        string
	value      string
	offset     int64
	offsetFile *os.File
}

// Init 初始化kafka客户端
func Init(addr []string, chanMaxSize int, wg *sync.WaitGroup) (err error) {
	totalLog, _ = os.OpenFile("./logsFinal/total.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// 生产者配置
	// config := sarama.NewConfig()
	// config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据leader和follow都要确认
	// config.Producer.Partitioner = sarama.NewRandomPartitioner // 分区选取规则，随机选取
	// config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回
	// config.Producer.Retry.Max = 10                            // 消息重试次数

	// // 与kafka建立连接
	// client, err := sarama.NewSyncProducer(addr, config)
	// if err != nil {
	// 	fmt.Printf("connect to kafka failed, err:%v \n", err)
	// 	return
	// }
	// defer client.Close()
	logDataChan = make(chan *LogData, chanMaxSize)
	wg.Add(1)
	go sendToKafka(wg)
	return
}

func sendToKafka(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case logData, ok := <-logDataChan:
			if !ok {
				fmt.Println("goroutine kafka logDataChan closed")
				return
			}
			offset := logData.offset
			value := logData.value
			offsetFile := logData.offsetFile
			offsetFileName := offsetFile.Name()
			fmt.Printf("偏移文件名%s 偏移量%d \n", offsetFileName, offset)

			fmt.Fprintln(totalLog, value)
			fmt.Fprintln(offsetFile, offset)

			// fmt.Println("输出内容->", value, offset)
			// fmt.Println("consume log:", logData.value, logData.offset)
			// msgObj := &sarama.ProducerMessage{
			// 	Topic: logData.topic,
			// 	Value: sarama.StringEncoder(logData.value),
			// }
			// _, _, err := client.SendMessage(msgObj)
			// if err != nil {
			// 	fmt.Printf("send message failed, err:%v \n", err)
			// 	os.Exit(1)
			// }
			// fmt.Fprintln(ofsFile, "ok")
			// ioutil.WriteFile(logData.offsetFile.Name(), []byte(offsetStr), 0644)
			// _, err := logData.offsetFile.WriteString(offsetStr)
			// if err != nil {
			// 	fmt.Printf("save offset to file failed, err:%v \n", err)
			// 	os.Exit(1)
			// }
		default:
			time.Sleep(time.Second)
		}
	}
}

// SendToChan 将消息发送到kafka的通道中
func SendToChan(topic, key, value string, offset int64, offsetFile *os.File) {
	logDataChan <- &LogData{
		topic:      topic,
		key:        key,
		value:      value,
		offset:     offset,
		offsetFile: offsetFile,
	}
}

// CloseChan 关闭通道
func CloseChan() {
	close(logDataChan)
}
