package client

import (
	"github.com/vburenin/firempq/export/proto"
)

type Message struct {
	id       string
	priority int64
	payload  string
	delay    int64
	ttl      int64
	syncWait bool
	async    bool
}

func NewMessage(payload string) *Message {
	return &Message{
		payload:  payload,
		id:       "",
		delay:    -1,
		ttl:      -1,
		syncWait: false,
		async:    false,
	}
}

func (msg *Message) SetId(id string) *Message {
	msg.id = id
	return msg
}

func (msg *Message) SetPriority(priority int64) *Message {
	msg.priority = priority
	return msg
}

func (msg *Message) SetDelay(delay uint64) *Message {
	msg.delay = int64(delay)
	return msg
}

func (msg *Message) SetTtl(ttl uint64) *Message {
	msg.ttl = int64(ttl)
	return msg
}

func (msg *Message) SetSyncWait(b bool) *Message {
	msg.syncWait = b
	return msg
}

func (msg *Message) SetAsync(b bool) *Message {
	msg.async = b
	return msg
}

func (msg *Message) sendMessageData(t *TokenUtil) error {
	if msg.id != "" {
		t.SendSpace()
		t.SendTokenWithSpace(proto.PrmID)
		t.SendString(msg.id)
	}
	if msg.delay >= 0 {
		t.SendSpace()
		t.SendTokenWithSpace(proto.PrmDelay)
		t.SendInt(msg.delay)
	}
	if msg.ttl >= 0 {
		t.SendSpace()
		t.SendTokenWithSpace(proto.PrmMsgTTL)
		t.SendInt(msg.ttl)
	}
	if msg.syncWait {
		t.SendSpace()
		t.SendToken(proto.PrmSyncWait)
	}

	t.SendSpace()
	t.SendToken(proto.PrmPayload)
	return t.SendString(msg.payload)
}

type QueueMessage struct {
	Id       string
	Payload  string
	Receipt  string
	ExpireTs int64
	UnlockTs int64
	PopCount int64
}

func parsePoppedMessages(tokens []string) ([]*QueueMessage, error) {
	if len(tokens) == 0 {
		WrongMessageFormatError("No array header")
	}
	arraySize, err := ParseArraySize(tokens[0])
	msgs := make([]*QueueMessage, 0, arraySize)
	if err != nil {
		return nil, err
	}
	tokens = tokens[1:]
	for i := arraySize; i > 0; i-- {
		if len(tokens) == 0 {
			WrongMessageFormatError("Array with messages ends unexpectedly")
		}
		keysCount, err := ParseMapSize(tokens[0])
		if err != nil {
			return nil, err
		}
		tokens = tokens[1:]
		tokensNeeded := keysCount << 1
		if int64(len(tokens)) < tokensNeeded {
			return nil, WrongMessageFormatError("Message data ends unexpectedly")
		}
		msg, err := parseMessage(tokens[:tokensNeeded])
		if err != nil {
			return nil, err
		}
		tokens = tokens[tokensNeeded:]
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func parseMessage(tokens []string) (*QueueMessage, error) {
	msg := QueueMessage{}
	var err error

	idx := len(tokens) - 2
	for idx >= 0 {
		switch tokens[idx] {
		case "ID":
			msg.Id = tokens[idx+1]
		case "PL":
			msg.Payload = tokens[idx+1]
		case "RCPT":
			msg.Receipt = tokens[idx+1]
		case "UTS":
			msg.UnlockTs, err = ParseInt(tokens[idx+1])
		case "ETS":
			msg.ExpireTs, err = ParseInt(tokens[idx+1])
		case "POPCNT":
			msg.PopCount, err = ParseInt(tokens[idx+1])
		default:
		}
		if err != nil {
			return nil, err
		}
		idx -= 2
	}
	return &msg, nil
}
