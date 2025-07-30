package masscan

// todo: 测试一下如果是长时间下的扫描任务，如何处理进度和结果
import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os/exec"
	"regexp"
	"strconv"
	"sync"
	"time"

	"port_scanning/core/kafka"
	"port_scanning/core/kafka/message"
	"port_scanning/core/redis"
)

type MasscanScanner struct {
	cmd *exec.Cmd
	cfg *message.ScanTask
}

// NewMasscanScanner 创建一个新的MasscanScanner实例
func NewMasscanScanner() *MasscanScanner {
	return &MasscanScanner{}
}

func (ms *MasscanScanner) BuildMasscanCommand(conf *message.ScanTask) *MasscanScanner {
	args := []string{
		conf.IPs, // IPs or CIDR
		"-p" + conf.Ports,
	}

	// 设置扫描速率
	if conf.Bandwidth > 0 {
		args = append(args, "--rate", strconv.Itoa(conf.Bandwidth))
	} else {
		args = append(args, "--rate", "100") // 默认速率
	}

	// 设置超时
	if conf.Timeout > 0 {
		args = append(args, "--wait", strconv.Itoa(conf.Timeout))
	}

	// 添加其他有用的参数
	args = append(args, "--open") // 只显示开放端口

	ms.cmd = exec.Command("./masscan.exe", args...)
	ms.cfg = conf
	return ms
}

func (ms *MasscanScanner) GetCommand() *exec.Cmd {
	if ms.cmd == nil {
		log.Fatal("Masscan command has not been built yet")
	}
	return ms.cmd
}

func (m *MasscanScanner) GetConfig() *message.ScanTask {
	if m.cfg == nil {
		log.Fatal("Masscan configuration has not been set")
	}
	return m.cfg
}

// Start 启动扫描并处理进度和结果
func (ms *MasscanScanner) Start() error {
	return ms.executeMasscanWithProgress()
}

// executeMasscanWithProgress 执行masscan并处理进度和结果
func (ms *MasscanScanner) executeMasscanWithProgress() error {
	stdout, err := ms.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %v", err)
	}

	stderr, err := ms.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %v", err)
	}

	if err := ms.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start masscan: %v", err)
	}

	// 正则表达式定义
	progressRegex := regexp.MustCompile(`rate:\s+([\d.]+)-kpps,\s+([\d.]+)%\s+done,\s+waiting\s+(\d+)-secs,\s+found=(\d+)`)
	resultRegex := regexp.MustCompile(`Discovered open port (\d+)/(tcp|udp) on ([\d.]+)`)
	scanningRegex := regexp.MustCompile(`Scanning (\d+) hosts \[(\d+) ports/host\]`)
	remainingRegex := regexp.MustCompile(`rate:\s+([\d.]+)-kpps,\s+([\d.]+)%\s+done,\s+(\d+:\d+:\d+)\s+remaining,\s+found=(\d+)`)

	var wg sync.WaitGroup
	wg.Add(2)

	// 处理stdout（主要是结果）
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer([]byte{}, 1024*1024) // 设置缓冲区大小为1MB
		result := message.NewScanResult(ms.cfg.TaskID, "waiting")
		for scanner.Scan() {
			line := scanner.Text()
			log.Printf("result info: %s", line)

			// 解析扫描结果
			if matches := resultRegex.FindStringSubmatch(line); len(matches) == 4 {
				result.Discovered = append(result.Discovered, line)
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Error reading stdout: %v", err)
		}

		//如果输入停止，发送结果到Kafka
		result.Total = len(result.Discovered)
		if err := kafka.Publish(result); err != nil {
			log.Printf("Failed to publish scan result to Kafka: %v", err)
		}
	}()

	// 处理stderr（主要是进度信息）
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderr)
		scanner.Buffer([]byte{}, 1024*1024) // 设置缓冲区大小为1MB
		for scanner.Scan() {
			line := scanner.Text()
			log.Printf("progress info: %s", line)

			// 解析扫描目标信息
			if matches := scanningRegex.FindStringSubmatch(line); len(matches) == 3 {
				hosts, _ := strconv.Atoi(matches[1])
				ports, _ := strconv.Atoi(matches[2])
				msg := fmt.Sprintf("Scanning %d hosts with %d ports each", hosts, ports)
				updateScanProgress(message.RUNNING, ms.cfg.TaskID, msg, "total")
			}

			// 解析进度信息 - 等待格式: rate:  0.00-kpps, 100.00% done, waiting 8-secs, found=2
			if matches := progressRegex.FindStringSubmatch(line); len(matches) == 5 {
				rate := matches[1]
				progress := matches[2]
				waiting := matches[3]
				found := matches[4]

				progressMsg := fmt.Sprintf("Rate: %s kpps, Progress: %s%%, Waiting: %s secs, Found: %s ports",
					rate, progress, waiting, found)

				// 如果等待时间为0，表示扫描完成
				if waiting == "0" {
					updateScanProgress(message.COMPLETED, ms.cfg.TaskID, progressMsg, "completed")
					break
				}

				updateScanProgress(message.RUNNING, ms.cfg.TaskID, progressMsg, "waiting")
			}

			// 解析进度信息 - 剩余时间格式: rate:  0.00-kpps, 50.00% done, 202:55:45 remaining, found=1
			if matches := remainingRegex.FindStringSubmatch(line); len(matches) == 5 {
				rate := matches[1]
				progress := matches[2]
				remaining := matches[3]
				found := matches[4]

				progressMsg := fmt.Sprintf("Rate: %s kpps, Progress: %s%%, Remaining: %s, Found: %s ports",
					rate, progress, remaining, found)

				updateScanProgress(message.RUNNING, ms.cfg.TaskID, progressMsg, "progress")
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Error reading stderr: %v", err)
		}
	}()

	// 等待所有输出处理完成
	wg.Wait()

	// 等待命令完成
	if err := ms.cmd.Wait(); err != nil {
		// 扫描完成，更新最终状态
		updateScanProgress(message.FAILED, ms.cfg.TaskID, fmt.Sprintf("Scan failed: %v", err), "fail")
		return fmt.Errorf("masscan command failed: %v", err)
	}

	return nil
}

// updateScanProgress 更新扫描进度到Redis
func updateScanProgress(status message.MessageStatus, taskID, msg, typed string) {
	_ = redis.GetRedisClient().Set(context.Background(), fmt.Sprintf("%s/scan:progress:%s", typed, taskID),
		string(message.NewScanProgress(taskID, msg, status).GetValue()), time.Hour*24)
	return
}
