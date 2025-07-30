package validator

import (
	"errors"
	"log"
	"strconv"
	"strings"
)

// PortValidator 端口验证器
type PortValidator struct{}

// NewPortValidator 创建端口验证器
func NewPortValidator() *PortValidator {
	return &PortValidator{}
}

// ValidatePorts 验证端口字符串（支持单个端口、端口范围、多个端口）
func (v *PortValidator) ValidatePorts(ports string) error {
	if ports == "" {
		return errors.New("ports cannot be empty")
	}

	ports = strings.TrimSpace(ports)

	// 分割逗号分隔的端口
	portParts := strings.Split(ports, ",")

	totalPorts := 0
	for _, part := range portParts {
		part = strings.TrimSpace(part)

		var portCount int
		var err error

		// 检查是否是端口范围格式 (如: 80-100)
		if strings.Contains(part, "-") {
			portCount, err = v.validatePortRange(part)
		} else {
			// 单个端口
			err = v.validateSinglePort(part)
			portCount = 1
		}

		if err != nil {
			log.Println("Port validation error:", err)
			return err
		}

		totalPorts += portCount
	}

	// 限制总端口数量
	if totalPorts > 10000 {
		return errors.New("total number of ports cannot exceed 10000")
	}

	return nil
}

// validatePortRange 验证端口范围
func (v *PortValidator) validatePortRange(rangeStr string) (int, error) {
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		log.Println("Invalid port range format:", rangeStr)
		return 0, errors.New("invalid port range format, expected 'start-end'")
	}

	startPort, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		log.Println("Error parsing start port:", err)
		return 0, err
	}

	endPort, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		log.Println("Error parsing end port:", err)
		return 0, err
	}

	if err := v.validatePortNumber(startPort); err != nil {
		log.Println("Start port validation error:", err)
		return 0, err
	}

	if err := v.validatePortNumber(endPort); err != nil {
		log.Println("End port validation error:", err)
		return 0, err
	}

	if startPort > endPort {
		log.Println("Start port cannot be greater than end port:", startPort, ">", endPort)
		return 0, errors.New("start port cannot be greater than end port")
	}

	portCount := endPort - startPort + 1

	// 限制单个范围的端口数量
	if portCount > 5000 {
		log.Println("Port range too large:", startPort, "-", endPort)
		return 0, errors.New("port range cannot exceed 5000 ports")
	}

	return portCount, nil
}

// validateSinglePort 验证单个端口
func (v *PortValidator) validateSinglePort(portStr string) error {
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Println("Error parsing port:", err)
		return err
	}

	return v.validatePortNumber(port)
}

// validatePortNumber 验证端口号范围
func (v *PortValidator) validatePortNumber(port int) error {
	if port < 1 || port > 65535 {
		log.Println("Port number out of range:", port)
		return errors.New("port number must be between 1 and 65535")
	}
	return nil
}
