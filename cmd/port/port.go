package port

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
	"port_scanning/core/kafka/producer"
	"port_scanning/core/message"
)

var (
	ip        string
	port      string
	bandwidth int
	wait      int
)

var StartCmd = &cobra.Command{
	Use:     "scan",
	Short:   "port scanning command",
	Example: "go run main.go scan -i 0.0.0.0 -p 80 -b 100",
	Run: func(cmd *cobra.Command, args []string) {
		if err := load(); err != nil {
			log.Println(err.Error())
			os.Exit(1)
		}
	},
}

func init() {
	StartCmd.PersistentFlags().StringVarP(&ip, "ip", "i", "121.43.175.130", "Specify the IP or IP range")
	StartCmd.PersistentFlags().StringVarP(&port, "port", "p", "80,443", "Specify the port")
	StartCmd.PersistentFlags().IntVarP(&bandwidth, "bandwidth", "b", 100, "Specify the scanning bandwidth in Mbps")
	StartCmd.PersistentFlags().IntVarP(&wait, "wait", "w", 5, "Specify the wait time in seconds")
}

func load() error {
	log.Println("Message is Produceing:")
	log.Println("ip:", ip, "port:", port, "bandwidth: ", bandwidth, "wait:", wait)

	//创建Kafka发布器
	Producer, err := producer.NewProducer([]string{"127.0.0.1:9092"},
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

	// 创建扫描任务
	scanTask := message.NewScanTask(ip, port, bandwidth, wait)

	// 发布扫描任务
	if err = Producer.Produce(context.Background(), scanTask); err != nil {
		log.Println("produce error: ", err)
		return err
	}

	log.Println("Scan task Produceed successfully")

	// 关闭发布器
	if err = Producer.Close(); err != nil {
		log.Println("Failed to close Kafka Producer:", err)
		return err
	}

	log.Println("Kafka Producer closed successfully")

	return nil
}
