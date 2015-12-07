package dsqueue

import (
	"firempq/common"

	. "firempq/api"
)

const (
	MAX_MESSAGE_ID_LENGTH = 64
)

type DSQMessage struct {
	Id string
	DSQueueMsgData
}

func NewDSQMessage(id string) *DSQMessage {
	return &DSQMessage{id, DSQueueMsgData{common.Uts(), 0, 0, 0, 0, 0}}
}

func UnmarshalDSQMessage(msgId string, buf []byte) *DSQMessage {
	msg := DSQMessage{msgId, DSQueueMsgData{}}
	msg.Unmarshal(buf)
	return &msg
}

func (m *DSQMessage) GetId() string {
	return m.Id
}

type MsgItem struct {
	msgMeta *DSQMessage
	payload string
}

func NewMsgItem(pqMsg *DSQMessage, payload string) *MsgItem {
	return &MsgItem{pqMsg, payload}
}

func (m *MsgItem) GetId() string {
	return m.msgMeta.GetId()
}

func (m *MsgItem) GetPayload() string {
	return m.payload
}

var _ IItem = &MsgItem{}
var _ IItemMetaData = &DSQMessage{}
