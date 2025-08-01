package test

import (
	"fmt"
	"log"
	"testing"

	"port_scanning/core/masscan"
	"port_scanning/core/message"
)

// go run main.go scan -i 121.43.175.130 -p1-2000
func TestMasscanMonitor(t *testing.T) {
	fmt.Println("Starting port scan monitoring...")

	err := masscan.PortScanMonitor("task-id-xxxx", "121.43.175.130", "1-2000", 100, 5, progressCallback)
	if err != nil {
		log.Println("Error during port scan monitoring:", err)
		return
	}

	log.Println("Port scan monitoring completed.")
}

func progressCallback(progress *message.ScanProgress) {
	fmt.Printf("Rate: %.2f kpps, Progress: %.2f%%, Remaining: %s, Found: %d\n",
		progress.Rate, progress.Progress, progress.Remaining, progress.Found)

	//if len(progress.OpenPorts) > 0 {
	//	fmt.Printf("Open ports: %v\n", progress.OpenPorts)
	//}
}
