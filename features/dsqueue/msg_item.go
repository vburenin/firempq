package dsqueue

import (
	"firempq/common"
)

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

func (m *MsgItem) GetContent() string {
	return m.payload
}

var _ common.IItem = &MsgItem{}
