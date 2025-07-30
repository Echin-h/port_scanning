package port

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"port_scanning/core/kafka"
	"port_scanning/core/kafka/message"
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
			fmt.Println(err.Error())
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
	fmt.Println("Message is publishing:")
	fmt.Println("ip:", ip, "port:", port, "bandwidth: ", bandwidth, "wait:", wait)

	//创建Kafka发布器
	publisher, err := kafka.NewKafkaPublisher()
	if err != nil {
		return err
	}

	// 创建扫描任务
	scanTask := message.NewScanTask(ip, port, bandwidth, wait)

	// 发布扫描任务
	if err = publisher.Publish(context.Background(), scanTask); err != nil {
		return fmt.Errorf("failed to publish scan task: %s", err.Error())
	}

	return nil
}
