package masscan

// todo: 测试一下如果是长时间下的扫描任务，如何处理进度和结果
import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"time"

	"port_scanning/core/message"
	"port_scanning/core/redis"
)

type Scanner struct {
	cmd *exec.Cmd
	cfg *message.ScanTask
}

// NewMasscanScanner 创建一个新的MasscanScanner实例
func NewMasscanScanner() *Scanner {
	return &Scanner{}
}

func (ms *Scanner) BuildMasscanCommand(conf *message.ScanTask) *Scanner {
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

func (ms *Scanner) GetCommand() *exec.Cmd {
	if ms.cmd == nil {
		log.Fatal("Masscan command has not been built yet")
	}
	return ms.cmd
}

func (m *Scanner) GetConfig() *message.ScanTask {
	if m.cfg == nil {
		log.Fatal("Masscan configuration has not been set")
	}
	return m.cfg
}

// Start 启动扫描并处理进度和结果
func (ms *Scanner) Start() error {
	return ms.executeMasscanWithProgress()
}

// executeMasscanWithProgress 执行masscan并处理进度和结果
func (ms *Scanner) executeMasscanWithProgress() error {
	return PortScanMonitor(ms.cfg.TaskID, ms.cfg.IPs, ms.cfg.Ports, ms.cfg.Bandwidth, ms.cfg.Timeout, progressCallback)
}

// updateScanProgress 更新扫描进度到Redis
// 格式: redis.LPush("scan-progress/{task_id}", message)
func updateScanProgress(message message.Message) {
	_ = redis.GetRedisClient().LPush(context.Background(), fmt.Sprintf("%s/%s", message.GetTopic(), message.GetKey()),
		message.GetValue(), 365*24*time.Hour)
	return
}

func progressCallback(progress *message.ScanProgress) {
	fmt.Printf("Rate: %.2f kpps, Progress: %.2f%%, Remaining: %s, Found: %d\n",
		progress.Rate, progress.Progress, progress.Remaining, progress.Found)
}
