package pqueue

import "firempq/common"
import "firempq/log"

const (
	MAX_MESSAGE_ID_LENGTH = 64
)

type PQMessage struct {
	Id string
	PQueueMsgData
}

func NewPQMessage(id string, priority int64) *PQMessage {
	return &PQMessage{id, PQueueMsgData{priority, common.Uts(), 0, 0}}
}

func UnmarshalPQMessage(msgId string, buf []byte) *PQMessage {
	p := PQMessage{Id: msgId}
	if err := p.Unmarshal(buf); err != nil {
		log.Error("Could not unmarshal message: %s", msgId)
		return nil
	}
	return &p
}

func (self *PQMessage) GetId() string {
	return self.Id
}

type MsgItem struct {
	msgMeta *PQMessage
	payload string
}

func NewMsgItem(pqMsg *PQMessage, payload string) *MsgItem {
	return &MsgItem{pqMsg, payload}
}

func (m *MsgItem) GetId() string {
	return m.msgMeta.GetId()
}

func (m *MsgItem) GetPayload() string {
	return m.payload
}

var _ common.IItem = &MsgItem{}
var _ common.IItemMetaData = &PQMessage{}
