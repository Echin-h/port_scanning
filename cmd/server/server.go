package server

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	_kafka "port_scanning/core/kafka"
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
		stop()
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

	// 创建Kafka接收器
	kafkaConsumer, err := _kafka.NewKafkaConsumer()
	if err != nil {
		log.Println("Failed to create Kafka Consumer:", err.Error())
		return err
	}

	// 注册扫描任务处理回调函数
	err = kafkaConsumer.RegisterCallback("scan-tasks", scanTaskMessageCallback)
	if err != nil {
		log.Println("Failed to register scan task callback:", err.Error())
		return err
	}

	// 启动接收器
	err = kafkaConsumer.Start()
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

	if err := kafkaConsumer.Stop(); err != nil {
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

func stop() {

}
