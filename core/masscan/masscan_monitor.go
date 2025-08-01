package masscan

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"port_scanning/core/kafka/producer"
	"port_scanning/core/message"
)

// PortScanMonitor 实时监控端口扫描进度
func PortScanMonitor(taskID string, target string, portRange string, bandwidth int, timeout int, callback func(*message.ScanProgress)) error {
	// 构建命令
	cmd := exec.Command("./masscan.exe", target, fmt.Sprintf("-p%s", portRange),
		"--rate", strconv.Itoa(bandwidth), "--wait", strconv.Itoa(timeout), "--open")

	cmd.Dir = "F:/port_scanning/" // 设置命令的工作目录

	// 获取标准输出和标准错误管道
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err = cmd.Start(); err != nil {
		return err
	}

	progressRegex := regexp.MustCompile(`rate:\s+([\d.]+)-kpps,\s+([\d.]+)% done,\s+(.+?) remaining, found=(\d+)`)
	portRegex := regexp.MustCompile(`Discovered open port (\d+/\w+) on (.+)`)

	progress := message.NewScanProgress(taskID)
	result := message.NewScanResult(taskID)

	var openPorts []string

	// 创建通道来处理两个输出流
	outputChan := make(chan string, 100)
	var wg sync.WaitGroup

	// 处理stdout的goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			outputChan <- line
		}
	}()

	// 处理stderr的goroutine - rate信息很可能在这里
	wg.Add(1)
	go func() {
		defer wg.Done()

		// 使用字节读取来处理可能的\r字符
		buf := make([]byte, 1024)
		var lineBuffer strings.Builder

		for {
			n, err := stderr.Read(buf)
			if err != nil {
				if err == io.EOF {
					break
				}
				continue
			}

			data := string(buf[:n])

			// 处理每个字符
			for _, char := range data {
				if char == '\r' {
					// 遇到回车符，处理当前行
					if lineBuffer.Len() > 0 {
						outputChan <- lineBuffer.String()
						lineBuffer.Reset()
					}
				} else if char == '\n' {
					// 遇到换行符，处理当前行
					if lineBuffer.Len() > 0 {
						outputChan <- lineBuffer.String()
						lineBuffer.Reset()
					}
				} else {
					// 普通字符，添加到缓冲区
					lineBuffer.WriteRune(char)
				}
			}
		}

		// 处理最后一行（如果有的话）
		if lineBuffer.Len() > 0 {
			outputChan <- lineBuffer.String()
		}
	}()

	// 处理输出的主循环
	go func() {
		for line := range outputChan {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			// 匹配进度信息
			if matches := progressRegex.FindStringSubmatch(line); matches != nil {
				rate, _ := strconv.ParseFloat(matches[1], 64)
				progressPercent, _ := strconv.ParseFloat(matches[2], 64)
				remaining := matches[3]
				found, _ := strconv.Atoi(matches[4])

				progress.Rate = rate
				progress.Progress = progressPercent
				progress.Remaining = remaining
				progress.Found = found

				if callback != nil {
					callback(progress)
				}

				// 更新扫描进度到Redis
				updateScanProgress(progress)
			}

			// 匹配发现的端口
			if matches := portRegex.FindStringSubmatch(line); matches != nil {
				port := matches[1]
				host := matches[2]
				openPort := fmt.Sprintf("%s on %s", port, host)
				openPorts = append(openPorts, openPort)

				progress.OpenPorts = append([]string{}, openPorts...)
			}
		}
	}()

	// 等待两个读取goroutine完成
	wg.Wait()

	time.Sleep(1 * time.Second) // 确保所有输出都被处理完

	close(outputChan)

	progress.Progress = 100.0
	// 回调函数画图
	callback(progress)
	// 更新最终的扫描进度到Redis
	updateScanProgress(progress)
	result.OpenPort = openPorts
	// 执行结果放入kafka
	prod, err := producer.GetProducer()
	if err != nil {
		return err
	}
	if err := prod.Produce(context.Background(), result); err != nil {
		return err
	}

	// 等待命令完成
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("command execution failed: %v", err)
	}

	return nil
}
