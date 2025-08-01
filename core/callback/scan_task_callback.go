package callback

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/segmentio/kafka-go"
	"port_scanning/core/masscan"
	"port_scanning/core/message"
)

func ScanTaskMessageCallback(_ context.Context, msg *kafka.Message) error {
	if msg == nil {
		return errors.New("consumed nil message")
	}

	var st = &message.ScanTask{}
	_ = json.Unmarshal(msg.Value, st)
	ms := masscan.NewMasscanScanner().BuildMasscanCommand(st)

	log.Println("开始执行命令: ", ms.GetCommand().String())
	if err := ms.Start(); err != nil {
		log.Println("failed to start masscan:", err.Error())
		return err
	}

	return nil
}
