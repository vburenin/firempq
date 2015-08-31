package pqueue

import (
	"encoding/binary"
	"firempq/common"
	"firempq/util"
)

const (
	MAX_MESSAGE_ID_LENGTH = 64
)

type PQMessage struct {
	Id        string
	Priority  int64
	CreatedTs int64
	PopCount  int64
	UnlockTs  int64
}

func NewPQMessageWithId(id string, priority int64) *PQMessage {

	pqm := PQMessage{
		Id:        id,
		Priority:  priority,
		CreatedTs: util.Uts(),
		PopCount:  0,
		UnlockTs:  0,
	}
	return &pqm
}

func PQMessageFromBinary(msgId string, buf []byte) *PQMessage {
	bufOffset := 0
	priority := binary.BigEndian.Uint64(buf[bufOffset:])

	bufOffset += 8
	createdTs := binary.BigEndian.Uint64(buf[bufOffset:])

	bufOffset += 8
	popCount := binary.BigEndian.Uint64(buf[bufOffset:])

	bufOffset += 8
	unlockTs := binary.BigEndian.Uint64(buf[bufOffset:])

	return &PQMessage{
		Id:        msgId,
		Priority:  int64(priority),
		CreatedTs: int64(createdTs),
		PopCount:  int64(popCount),
		UnlockTs:  int64(unlockTs),
	}
}

func (pqm *PQMessage) GetId() string {
	return pqm.Id
}

func (pqm *PQMessage) ToBinary() []byte {
	// length of 4 64 bits integers.

	buf := make([]byte, 8*4)

	bufOffset := 0
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(pqm.Priority))

	bufOffset += 8
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(pqm.CreatedTs))

	bufOffset += 8
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(pqm.PopCount))

	bufOffset += 8
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(pqm.UnlockTs))

	return buf
}

func (pqm *PQMessage) GetStatus() map[string]interface{} {
	res := make(map[string]interface{})
	res["CreatedTs"] = pqm.CreatedTs
	res["Priority"] = pqm.Priority
	res["PopCount"] = pqm.PopCount
	res["UnlockTs"] = pqm.UnlockTs
	return res
}

var _ common.IItemMetaData = &PQMessage{}
