package server

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"port_scanning/core/callback"
	"port_scanning/core/kafka/consumer"
	"port_scanning/core/kafka/producer"
	"port_scanning/core/redis"
)

var StartCmd = &cobra.Command{
	Use:     "server",
	Short:   "port scanning server",
	Example: "go run main.go server",
	Run: func(cmd *cobra.Command, args []string) {
		if err := load(); err != nil {
			log.Println(err.Error())
			os.Exit(1)
		}
	},
}

func init() {

}

func load() error {
	// 初始化Redis客户端
	var c *redis.RedisConfig
	redis.NewRedisClient(c)

	if err := redis.GetRedisClient().Ping(context.Background()); err != nil {
		log.Println("Failed to connect to Redis:", err.Error())
		return err
	}

	log.Println("Redis client loaded successfully")

	// 创建Kafka发布器
	_, err := producer.NewProducer([]string{"127.0.0.1:9092"},
		producer.WithRequiredAcks(-1),
		producer.WithAsync(false),
		producer.WithProducerReadTimeout(10*time.Second),
		producer.WithProducerWriteTimeout(10*time.Second),
		producer.WithAutoTopicCreation(true),
	)
	if err != nil {
		log.Println("Failed to create Kafka Producer:", err.Error())
		return err
	}
	log.Println("Kafka Producer created successfully")

	// 创建Kafka接收器
	groupID := "scan-cons-group"
	cfg, err := consumer.NewConsumerConfig([]string{"127.0.0.1:9092"},
		consumer.WithGroupID(groupID),
		consumer.WithCommitInterval(0),
		consumer.WithLatestOffset(),
		//kafka.WithEarliestOffset(),
		consumer.WithMaxBytes(10*1024*1024), // 10MB
		consumer.WithMinBytes(1),            // 10KB
		consumer.WithDefaultLogger(),
		consumer.WithDefaultErrorLogger(),
		consumer.WithRoundRobinGroupBalancer(),
		consumer.WithHeartbeatInterval(1*time.Second),
		consumer.WithSessionTimeout(10*time.Second),
	)

	if err != nil {
		log.Println("Failed to create Kafka Consumer:", err.Error())
		return err
	}

	asyncCfg := &consumer.AsyncConfig{
		WorkerCount: 10,   // 启动10个工作协程处理消息
		QueueSize:   1000, // 每个工作协程的消息队
	}

	cons := consumer.NewConsumer(cfg, asyncCfg)

	// 注册扫描任务处理回调函数
	err = cons.RegisterCallback("scan-tasks", callback.ScanTaskMessageCallback)
	if err != nil {
		log.Println("Failed to register scan task callback:", err.Error())
		return err
	}

	// 启动接收器
	err = cons.Start()
	if err != nil {
		log.Println("Failed to start Kafka Consumer:", err.Error())
		return err
	}

	log.Println("Scan task processor is running...")

	// 设置信号监听，实现优雅关闭
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-signalChan
	log.Printf("Consumed signal: %s, shutting down...\n", sig)

	if err := cons.Stop(); err != nil {
		log.Println("Failed to stop Kafka Consumer:", err.Error())
		return err
	}

	if err := redis.GetRedisClient().Close(); err != nil {
		log.Println("Failed to close Redis client:", err.Error())
		return err
	}

	log.Println("Server stopped gracefully")

	return nil
}
