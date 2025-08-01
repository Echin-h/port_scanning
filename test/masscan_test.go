package test

import (
	"bufio"
	"fmt"
	"log"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

// RateInfo 存储解析出的rate信息
type RateInfo struct {
	Rate      string    // 速率 (如: 0.10-kpps)
	Progress  string    // 进度百分比 (如: 10.74%)
	TimeLeft  string    // 剩余时间 (如: 0:01:28)
	Found     int       // 找到的端口数量
	Timestamp time.Time // 时间戳
}

// TestMasscanSimpleRateExtraction 简化版本的rate数据提取
func TestMasscanSimpleRateExtraction(t *testing.T) {
	fmt.Println("开始执行masscan并实时提取rate数据...")

	// 执行masscan命令
	cmd := exec.Command("./masscan.exe", "121.43.175.130", "-p1-10000")
	cmd.Dir = "F:/port_scanning/" // 设置命令的工作目录

	// 合并stdout和stderr
	cmd.Stderr = cmd.Stdout
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("无法获取输出管道: %v", err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("无法启动masscan: %v", err)
	}

	// 实时读取输出
	scanner := bufio.NewScanner(stdout)
	// 修正正则表达式以匹配实际的masscan输出格式
	// rate:  0.10-kpps,  1.62% done,   0:06:09 remaining, found=0
	rateRegex := regexp.MustCompile(`rate:\s+([^,]+),\s+([\d.]+%)\s+done,\s+([^,]+)\s+remaining,\s+found=(\d+)`)

	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		lineCount++

		// 显示所有输出（可选）
		fmt.Printf("[%d] %s\n", lineCount, line)

		// 检查是否为rate行
		if strings.Contains(line, "rate:") {
			fmt.Printf("🔍 检测到rate行: %s\n", line)
			matches := rateRegex.FindStringSubmatch(line)
			if len(matches) == 5 {
				fmt.Printf("📊 提取到Rate数据: 速率=%s, 进度=%s, 剩余时间=%s, 发现端口=%s\n",
					strings.TrimSpace(matches[1]),
					strings.TrimSpace(matches[2]),
					strings.TrimSpace(matches[3]),
					strings.TrimSpace(matches[4]))
			} else {
				fmt.Printf("❌ 正则匹配失败，匹配组数量: %d\n", len(matches))
				if len(matches) > 0 {
					fmt.Printf("匹配结果: %v\n", matches)
				}
			}
		}
	}

	// 等待命令完成
	if err := cmd.Wait(); err != nil {
		fmt.Printf("masscan执行完成，退出状态: %v\n", err)
	} else {
		fmt.Println("masscan执行成功完成！")
	}
}

// TestMasscan 原有的测试函数，将输出重定向到文件
func TestMasscan(t *testing.T) {
}

// TestRateRegex 测试正则表达式匹配
func TestRateRegex(t *testing.T) {
	testLines := []string{
		"rate:  0.01-kpps,  0.01% done,3475177:11:46 remaining, found=0",
		"rate:  0.10-kpps,  0.86% done,   0:11:49 remaining, found=0",
		"rate:  0.10-kpps,  1.62% done,   0:06:09 remaining, found=0",
		"rate:  0.10-kpps, 10.74% done,   0:01:28 remaining, found=0",
		"rate:  0.10-kpps, 15.28% done,   0:01:24 remaining, found=0",
	}

	for i, line := range testLines {
		fmt.Printf("测试行 %d: %s\n", i+1, line)
		rateInfo, err := parseRateLine(line)
		if err != nil {
			fmt.Printf("  ❌ 解析失败: %v\n", err)
		} else {
			fmt.Printf("  ✅ 解析成功: 速率=%s, 进度=%s, 剩余时间=%s, 发现端口=%d\n",
				rateInfo.Rate, rateInfo.Progress, rateInfo.TimeLeft, rateInfo.Found)
		}
		fmt.Println()
	}
}

// parseRateLine 解析rate行数据
func parseRateLine(line string) (*RateInfo, error) {
	// 匹配rate行的正则表达式，修正以适应实际格式
	// rate:  0.10-kpps,  1.62% done,   0:06:09 remaining, found=0
	re := regexp.MustCompile(`rate:\s+([^,]+),\s+([\d.]+%)\s+done,\s+([^,]+)\s+remaining,\s+found=(\d+)`)

	matches := re.FindStringSubmatch(line)
	if len(matches) != 5 {
		return nil, fmt.Errorf("无法解析rate行: %s (匹配组数量: %d)", line, len(matches))
	}

	found := 0
	fmt.Sscanf(matches[4], "%d", &found)

	return &RateInfo{
		Rate:      strings.TrimSpace(matches[1]),
		Progress:  strings.TrimSpace(matches[2]),
		TimeLeft:  strings.TrimSpace(matches[3]),
		Found:     found,
		Timestamp: time.Now(),
	}, nil
} // TestMasscanRealTimeExtraction 实时提取masscan的rate数据
func TestMasscanRealTimeExtraction(t *testing.T) {
	// 构建masscan命令
	cmd := exec.Command("./masscan.exe", "121.43.175.130", "-p1-10000")

	// 获取命令的stdout和stderr管道
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("无法获取stdout管道: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("无法获取stderr管道: %v", err)
	}

	// 启动命令
	if err := cmd.Start(); err != nil {
		t.Fatalf("无法启动masscan: %v", err)
	}

	// 创建用于接收rate信息的通道
	rateChan := make(chan *RateInfo, 100)
	done := make(chan bool)

	// 启动goroutine处理stdout
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("STDOUT: %s\n", line)

			if strings.Contains(line, "rate:") {
				if rateInfo, err := parseRateLine(line); err == nil {
					rateChan <- rateInfo
				} else {
					log.Printf("解析rate行失败: %v", err)
				}
			}
		}
	}()

	// 启动goroutine处理stderr
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("STDERR: %s\n", line)

			if strings.Contains(line, "rate:") {
				if rateInfo, err := parseRateLine(line); err == nil {
					rateChan <- rateInfo
				} else {
					log.Printf("解析rate行失败: %v", err)
				}
			}
		}
	}()

	// 启动goroutine监控命令完成
	go func() {
		_ = cmd.Wait()
		close(rateChan)
		done <- true
	}()

	// 实时处理rate信息
	fmt.Println("开始实时监控masscan扫描进度...")
	fmt.Println("时间戳\t\t\t速率\t\t进度\t\t剩余时间\t\t发现端口")
	fmt.Println(strings.Repeat("-", 80))

	for {
		select {
		case rateInfo := <-rateChan:
			if rateInfo != nil {
				fmt.Printf("%s\t%s\t\t%s\t\t%s\t\t%d\n",
					rateInfo.Timestamp.Format("15:04:05"),
					rateInfo.Rate,
					rateInfo.Progress,
					rateInfo.TimeLeft,
					rateInfo.Found,
				)
			}
		case <-done:
			fmt.Println("扫描完成!")
			return
		case <-time.After(5 * time.Minute): // 超时保护
			fmt.Println("扫描超时，停止监控")
			_ = cmd.Process.Kill()
			return
		}
	}
}
