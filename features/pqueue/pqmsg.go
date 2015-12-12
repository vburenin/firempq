package pqueue

import (
	. "firempq/api"
	"firempq/common"
	"firempq/log"
)

const (
	MAX_MESSAGE_ID_LENGTH = 64
)

type PQMsgMetaData struct {
	Id string
	PQueueMsgData
}

func NewPQMsgMetaData(id string, priority int64, serialNumber uint64) *PQMsgMetaData {
	return &PQMsgMetaData{id, PQueueMsgData{priority, common.Uts(), 0, 0, serialNumber}}
}

func UnmarshalPQMsgMetaData(msgId string, buf []byte) *PQMsgMetaData {
	p := PQMsgMetaData{Id: msgId}
	if err := p.Unmarshal(buf); err != nil {
		log.Error("Could not unmarshal message: %s", msgId)
		return nil
	}
	return &p
}

func (self *PQMsgMetaData) GetId() string {
	return self.Id
}

func (self *PQMsgMetaData) StringMarshal() string {
	data, _ := self.Marshal()
	return common.UnsafeBytesToString(data)
}

type PQPayloadData struct {
	Id      string
	payload string
}

func NewMsgPayloadData(id string, payload string) *PQPayloadData {
	return &PQPayloadData{id, payload}
}

func (m *PQPayloadData) GetId() string {
	return m.Id
}

func (m *PQPayloadData) GetPayload() string {
	return m.payload
}

var _ IItem = &PQPayloadData{}
var _ IItemMetaData = &PQMsgMetaData{}
