package dsqueue

import (
	"firempq/common"
	"firempq/defs"
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

func (m *MsgItem) GetContentType() defs.DataType {
	return defs.DT_STR
}

func (m *MsgItem) GetStatus() map[string]interface{} {
	return m.msgMeta.GetStatus()
}

var _ common.IItem = &MsgItem{}
