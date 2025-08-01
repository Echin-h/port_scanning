package test

import (
	"fmt"
	"log"
	"os/exec"
	"sync"
	"testing"
)

// go run main.go scan 121.43.175.130 -p1-10000 -b 100
func TestScan(t *testing.T) {
	total := 1000    // 总执行次数
	concurrency := 5 // 并发数（可根据需求调整）
	execPath := "F:/port_scanning/"

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency) // 控制并发量

	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{} // 获取信号量

		go func(index int) {
			defer wg.Done()
			defer func() { <-sem }() // 释放信号量

			fmt.Printf("执行第 %d 次\n", index+1)
			//cmd := exec.Command("go", "run", "main.go", "scan", "121.43.175.130", "-p1-10000", "-b", "100")
			cmd := exec.Command("go", "run", "main.go", "scan", "-i", "100.40.100.10")
			cmd.Dir = execPath                  // 设置命令的工作目录
			output, err := cmd.CombinedOutput() // 捕获 stdout 和 stderr
			if err != nil {
				log.Printf("第 %d 次执行失败: %v\n输出: %s", index+1, err, output)
				return
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("所有执行完成")
}
