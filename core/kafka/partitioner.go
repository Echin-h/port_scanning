package kafka

//
//import (
//	"crypto/md5"
//	"encoding/binary"
//	"fmt"
//	"net"
//	"strings"
//)
//
//// Partitioner 通用分片器接口
//type Partitioner interface {
//	// GetPartition 根据key获取分区号
//	GetPartition(key string, numPartitions int) (int, error)
//	// GetPartitionKey 获取用于分区的key（可能对原key进行转换）
//	GetPartitionKey(key string) (string, error)
//	// Name 返回分片器名称
//	Name() string
//}
//
//// IPSegmentPartitioner IP段分区器，用于将不同IP段均匀分配到不同分区
//type IPSegmentPartitioner struct {
//	segmentBits int // 子网掩码位数，用于单个IP的网段划分
//}
//
//// NewIPSegmentPartitioner 创建一个新的IP段分区器
//func NewIPSegmentPartitioner(key int) *IPSegmentPartitioner {
//	if key <= 0 || key > 32 {
//		key = 24 // 默认使用/24网段
//	}
//	return &IPSegmentPartitioner{
//		segmentBits: key,
//	}
//}
//
//// GetPartition 根据IP相关的key获取分区号
//func (p *IPSegmentPartitioner) GetPartition(key string, numPartitions int) (int, error) {
//	if numPartitions <= 0 {
//		return 0, fmt.Errorf("分区数量必须大于0")
//	}
//
//	segmentKey, err := p.GetPartitionKey(key)
//	if err != nil {
//		return 0, err
//	}
//
//	// 使用MD5哈希确保分布更均匀
//	hash := md5.Sum([]byte(segmentKey))
//	hashValue := binary.BigEndian.Uint32(hash[:4])
//	return int(hashValue % uint32(numPartitions)), nil
//}
//
//// GetPartitionKey 根据输入的IP格式获取用于分区的key
//func (p *IPSegmentPartitioner) GetPartitionKey(key string) (string, error) {
//	key = strings.TrimSpace(key)
//
//	// 检查是否是CIDR格式
//	if strings.Contains(key, "/") {
//		return p.getCIDRKey(key)
//	}
//
//	// 检查是否是IP范围格式
//	if strings.Contains(key, "-") {
//		return p.getIPRangeKey(key)
//	}
//
//	// 检查是否是逗号分隔的多个IP
//	if strings.Contains(key, ",") {
//		return p.getMultipleIPsKey(key)
//	}
//
//	// 单个IP
//	return p.getSingleIPKey(key)
//}
//
//// getCIDRKey 从CIDR格式获取分区键
//func (p *IPSegmentPartitioner) getCIDRKey(cidr string) (string, error) {
//	_, network, err := net.ParseCIDR(cidr)
//	if err != nil {
//		return "", fmt.Errorf("无效的CIDR格式: %s", cidr)
//	}
//
//	// 直接返回网络地址作为分区键
//	return network.String(), nil
//}
//
//// getIPRangeKey 从IP范围格式获取分区键
//func (p *IPSegmentPartitioner) getIPRangeKey(rangeStr string) (string, error) {
//	parts := strings.Split(rangeStr, "-")
//	if len(parts) != 2 {
//		return "", fmt.Errorf("无效的IP范围格式: %s", rangeStr)
//	}
//
//	startIP := net.ParseIP(strings.TrimSpace(parts[0]))
//	if startIP == nil {
//		return "", fmt.Errorf("无效的起始IP: %s", parts[0])
//	}
//
//	// 使用起始IP所在的网段作为分区键
//	startIPv4 := startIP.To4()
//	if startIPv4 == nil {
//		return "", fmt.Errorf("仅支持IPv4: %s", parts[0])
//	}
//
//	// 创建掩码并获取网段
//	mask := net.CIDRMask(p.segmentBits, 32)
//	network := &net.IPNet{
//		IP:   startIPv4.Mask(mask),
//		Mask: mask,
//	}
//	return network.String(), nil
//}
//
//// getMultipleIPsKey 从多个IP格式获取分区键
//func (p *IPSegmentPartitioner) getMultipleIPsKey(ips string) (string, error) {
//	// 对于多个IP，我们使用第一个IP所在的网段
//	ipList := strings.Split(ips, ",")
//	if len(ipList) == 0 {
//		return "", fmt.Errorf("IP列表为空")
//	}
//
//	firstIP := strings.TrimSpace(ipList[0])
//	return p.getSingleIPKey(firstIP)
//}
//
//// getSingleIPKey 从单个IP获取分区键
//func (p *IPSegmentPartitioner) getSingleIPKey(ip string) (string, error) {
//	parsedIP := net.ParseIP(ip)
//	if parsedIP == nil {
//		return "", fmt.Errorf("无效的IP地址: %s", ip)
//	}
//
//	ipv4 := parsedIP.To4()
//	if ipv4 == nil {
//		return "", fmt.Errorf("仅支持IPv4: %s", ip)
//	}
//
//	// 创建掩码并获取网段
//	mask := net.CIDRMask(p.segmentBits, 32)
//	network := &net.IPNet{
//		IP:   ipv4.Mask(mask),
//		Mask: mask,
//	}
//	return network.String(), nil
//}
//
//func (p *IPSegmentPartitioner) Name() string {
//	return fmt.Sprintf("IPSegmentPartitioner/%d", p.segmentBits)
//}
//
//type MockPartitioner struct {
//	name string
//}
//
//func NewMockPartitioner(name string) *MockPartitioner {
//	return &MockPartitioner{
//		name: name,
//	}
//}
//
//func (p *MockPartitioner) GetPartition(key string, numPartitions int) (int, error) {
//	return 0, nil
//}
//
//func (p *MockPartitioner) GetPartitionKey(key string) (string, error) {
//	return "", nil
//}
//
//func (p *MockPartitioner) Name() string {
//	return p.name
//}
