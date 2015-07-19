package pqueue

import (
	"encoding/binary"
	"firempq/common"
	"firempq/defs"
	"firempq/qerrors"
	"firempq/util"
	"strconv"
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

func MessageFromMap(params map[string]string) (*PQMessage, error) {
	// Get and check message id.
	var msgId string
	var ok bool
	var strValue string
	var priority int64
	var err error

	msgId, ok = params[defs.PRM_ID]
	if !ok {
		msgId = util.GenRandMsgId()
	} else if len(msgId) > MAX_MESSAGE_ID_LENGTH {
		return nil, qerrors.ERR_MSG_ID_TOO_LARGE
	}

	strValue, ok = params[defs.PRM_PRIORITY]
	if !ok {
		return nil, qerrors.ERR_MSG_NO_PRIORITY
	}
	priority, err = strconv.ParseInt(strValue, 10, 0)
	if err != nil {
		return nil, qerrors.ERR_MSG_WRONG_PRIORITY
	}

	return NewPQMessageWithId(msgId, priority), nil
}

func NewPQMessage(payload string, priority int64) *PQMessage {
	id := util.GenRandMsgId()
	return NewPQMessageWithId(id, priority)
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
