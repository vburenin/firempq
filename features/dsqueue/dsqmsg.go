package dsqueue

import (
	"firempq/common"

	. "firempq/api"
)

const (
	MAX_MESSAGE_ID_LENGTH = 64
)

type DSQMetaMessage struct {
	Id string
	DSQueueMsgData
}

func NewDSQMetaMessage(id string) *DSQMetaMessage {
	return &DSQMetaMessage{id, DSQueueMsgData{common.Uts(), 0, 0, 0, 0}}
}

func UnmarshalDSQMetaMessage(msgId string, buf []byte) *DSQMetaMessage {
	msg := DSQMetaMessage{msgId, DSQueueMsgData{}}
	msg.Unmarshal(buf)
	return &msg
}

func (m *DSQMetaMessage) GetId() string {
	return m.Id
}

func (m *DSQMetaMessage) StringMarshal() string {
	data, _ := m.Marshal()
	return common.UnsafeBytesToString(data)
}

type DSQPayloadData struct {
	Id      string
	payload string
}

func NewMsgItem(id string, payload string) *DSQPayloadData {
	return &DSQPayloadData{id, payload}
}

func (m *DSQPayloadData) GetId() string {
	return m.Id
}

func (m *DSQPayloadData) GetPayload() string {
	return m.payload
}

var _ IItem = &DSQPayloadData{}
var _ IItemMetaData = &DSQMetaMessage{}
