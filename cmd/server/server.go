package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"
	_kafka "port_scanning/core/kafka"
	"port_scanning/core/kafka/message"
	"port_scanning/core/masscan"
	"port_scanning/core/redis"
)

var StartCmd = &cobra.Command{
	Use:     "server",
	Short:   "port scanning server",
	Example: "go run main.go server",
	Run: func(cmd *cobra.Command, args []string) {
		if err := load(); err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	},
}

func init() {

}

func load() error {
	var c *redis.RedisConfig
	redis.NewRedisClient(c)

	if err := redis.GetRedisClient().Ping(context.Background()); err != nil {
		return fmt.Errorf("failed to connect to Redis: %s", err.Error())
	}

	fmt.Println("Redis client loaded successfully")

	// 创建Kafka接收器
	kafkaReceiver, err := _kafka.NewKafkaReceiver()
	if err != nil {
		return fmt.Errorf("failed to create Kafka receiver: %s", err.Error())
	}

	// 注册扫描任务处理回调函数
	err = kafkaReceiver.RegisterCallback("scan-tasks", handleScanTaskMessage)
	if err != nil {
		return fmt.Errorf("failed to register scan task callback: %v", err)
	}

	// 启动接收器
	err = kafkaReceiver.Start()
	if err != nil {
		return fmt.Errorf("failed to start Kafka receiver: %v", err)
	}

	log.Println("Scan task processor loaded successfully")

	// 阻塞等待接收器停止
	ch := make(chan struct{})
	<-ch

	return nil
}

// 处理扫描任务消息的回调函数
func handleScanTaskMessage(ctx context.Context, msg *kafka.Message) error {
	fmt.Println("-----------------------------------------------!!!!!!!!!!!!!!!!")
	if msg == nil {
		// 可以选择返回一个特定的错误，或者直接忽略
		return fmt.Errorf("received nil message")
		// 或者直接返回nil表示忽略
	}
	fmt.Println("key: ", string(msg.Key))
	fmt.Println("value: ", string(msg.Value))

	var st = &message.ScanTask{}
	_ = json.Unmarshal(msg.Value, st)
	ms := masscan.NewMasscanScanner().BuildMasscanCommand(st)

	fmt.Println(ms.GetCommand().String())
	err := ms.Start()
	if err != nil {
		log.Printf("Failed to execute scan task: %v", err)
		return err
	}
	return nil

	//log.Printf("Received scan task message: key=%s, partition=%d, offset=%d",
	//	string(msg.Key), msg.Partition, msg.Offset)
	//
	//// 解析扫描任务
	//var task ScanTask
	//if err := json.Unmarshal(msg.Value, &task); err != nil {
	//	return fmt.Errorf("failed to unmarshal scan task: %v", err)
	//}
	//
	//// 验证任务参数
	//if err := validateScanTask(&task); err != nil {
	//	return fmt.Errorf("invalid scan task: %v", err)
	//}
	//
	//// 异步执行扫描任务
	//go executeScanTask(task)
	//
	//log.Printf("Scan task %s queued for execution", task.TaskID)
	//return nil
}
