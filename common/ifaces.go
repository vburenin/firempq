package common

type IMessage interface {
	GetId() string
	GetStatus() map[string]interface{}
	ToBinary() []byte
}

type IBinaryItem interface {
	GetId() string
	ToBinary() []byte
}

type IQueue interface {
	PushMessage(msgData map[string]string, payload string) error
	PopMessage() (IMessage, error)
	GetMessagePayload(msgId string) string
	DeleteById(msgId string) error
	GetStatus() map[string]interface{}
	DeleteAll()
	GetQueueType() string
	CustomHandler(action string, params map[string]string) error
	PopWait(timeout, limit int) []IMessage
	Close()
}

type IQueueServer interface {
	Start()
	Stop()
}
