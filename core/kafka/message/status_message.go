package message

type MessageStatus string

const (
	WAITING   MessageStatus = "waiting"
	RUNNING                 = "running"
	COMPLETED               = "completed"
	FAILED                  = "failed"
)
