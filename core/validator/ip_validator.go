package validator

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

// IPValidator IP验证器
type IPValidator struct{}

// NewIPValidator 创建IP验证器
func NewIPValidator() *IPValidator {
	return &IPValidator{}
}

// ValidateIPs 验证IP字符串（支持单个IP、CIDR、IP范围、多个IP）
func (v *IPValidator) ValidateIPs(ips string) error {
	if ips == "" {
		return errors.New("ips cannot be empty")
	}

	ips = strings.TrimSpace(ips)

	// 检查是否是CIDR格式
	if strings.Contains(ips, "/") {
		return v.validateCIDR(ips)
	}

	// 检查是否是IP范围格式 (如: 192.168.1.1-192.168.1.10)
	if strings.Contains(ips, "-") {
		return v.validateIPRange(ips)
	}

	// 检查是否是逗号分隔的多个IP
	if strings.Contains(ips, ",") {
		return v.validateMultipleIPs(ips)
	}

	// 单个IP
	return v.validateSingleIP(ips)
}

// validateCIDR 验证CIDR格式
func (v *IPValidator) validateCIDR(cidr string) error {
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return errors.New("invalid CIDR format")
	}

	// 检查网络大小限制
	ones, bits := ipNet.Mask.Size()
	if bits-ones > 16 { // 限制最大65536个IP
		return errors.New("CIDR network too large (max /16)")
	}

	return nil
}

// validateIPRange 验证IP范围
func (v *IPValidator) validateIPRange(rangeStr string) error {
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return errors.New("invalid IP range format")
	}

	startIP := net.ParseIP(strings.TrimSpace(parts[0]))
	endIP := net.ParseIP(strings.TrimSpace(parts[1]))

	if startIP == nil {
		return errors.New("invalid start IP")
	}
	if endIP == nil {
		return errors.New("invalid end IP")
	}

	// 转换为IPv4
	startIPv4 := startIP.To4()
	endIPv4 := endIP.To4()

	if startIPv4 == nil || endIPv4 == nil {
		return errors.New("only IPv4 ranges are supported")
	}

	// 检查IP范围是否合理
	if compareIPs(startIPv4, endIPv4) > 0 {
		return errors.New("start IP is greater than end IP")
	}

	// 检查范围大小限制
	if v.calculateIPRangeSize(startIPv4, endIPv4) > 65536 {
		return errors.New("IP range too large (max 65536 IPs)")
	}

	return nil
}

// validateMultipleIPs 验证多个IP
func (v *IPValidator) validateMultipleIPs(ips string) error {
	ipList := strings.Split(ips, ",")

	if len(ipList) > 1000 { // 限制最大1000个IP
		return errors.New("too many IPs (max 1000)")
	}

	for i, ip := range ipList {
		ip = strings.TrimSpace(ip)
		if err := v.validateSingleIP(ip); err != nil {
			return errors.New(fmt.Sprintf("invalid IP at position %d: %s", i+1, err.Error()))
		}
	}

	return nil
}

// validateSingleIP 验证单个IP
func (v *IPValidator) validateSingleIP(ip string) error {
	if net.ParseIP(ip) == nil {
		return errors.New("invalid IP format")
	}
	return nil
}

// calculateIPRangeSize 计算IP范围大小
func (v *IPValidator) calculateIPRangeSize(startIP, endIP net.IP) int {
	start := ipToUint32(startIP)
	end := ipToUint32(endIP)
	return int(end - start + 1)
}

// 辅助函数
func compareIPs(ip1, ip2 net.IP) int {
	if ip1 == nil || ip2 == nil {
		return 0
	}

	for i := 0; i < len(ip1) && i < len(ip2); i++ {
		if ip1[i] < ip2[i] {
			return -1
		}
		if ip1[i] > ip2[i] {
			return 1
		}
	}
	return 0
}

func ipToUint32(ip net.IP) uint32 {
	ip = ip.To4()
	return uint32(ip[0])<<24 + uint32(ip[1])<<16 + uint32(ip[2])<<8 + uint32(ip[3])
}
