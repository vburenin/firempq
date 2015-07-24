package dsqueue

import (
	"encoding/binary"
	"firempq/common"
	"firempq/defs"
	"firempq/qerrors"
	"firempq/util"
)

const (
	MAX_MESSAGE_ID_LENGTH = 64
)

type DSQMessage struct {
	Id        	string
	CreatedTs 	int64
	PopCount  	int64
	UnlockTs  	int64
	DeliveryTs 	int64
	pushAt		uint8
}

func NewDSQMessageWithId(id string) *DSQMessage {

	pqm := DSQMessage{
		Id:        	id,
		CreatedTs: 	util.Uts(),
		PopCount:  	0,
		UnlockTs:  	0,
		DeliveryTs:	0,
	}
	return &pqm
}

func MessageFromMap(params map[string]string) (*DSQMessage, error) {
	// Get and check message id.
	var msgId string
	var ok bool

	msgId, ok = params[defs.PRM_ID]
	if !ok {
		msgId = util.GenRandMsgId()
	} else if len(msgId) > MAX_MESSAGE_ID_LENGTH {
		return nil, qerrors.ERR_MSG_ID_TOO_LARGE
	}

	return NewDSQMessageWithId(msgId), nil
}

func PQMessageFromBinary(msgId string, buf []byte) *DSQMessage {
	bufOffset := 0
	createdTs := binary.BigEndian.Uint64(buf[bufOffset:])

	bufOffset += 8
	popCount := binary.BigEndian.Uint64(buf[bufOffset:])

	bufOffset += 8
	unlockTs := binary.BigEndian.Uint64(buf[bufOffset:])

	bufOffset += 8
	deliveryTs := binary.BigEndian.Uint64(buf[bufOffset:])

	bufOffset += 8
	pushAt := uint8(buf[bufOffset])

	return &DSQMessage{
		Id:        	msgId,
		CreatedTs: 	int64(createdTs),
		PopCount:  	int64(popCount),
		UnlockTs:  	int64(unlockTs),
		DeliveryTs:	int64(deliveryTs),
		pushAt:		uint8(pushAt),
	}
}

func (pqm *DSQMessage) GetId() string {
	return pqm.Id
}

func (pqm *DSQMessage) ToBinary() []byte {
	// length of 4 64 bits integers ann one 8 bit.

	buf := make([]byte, 8*4 + 1)

	bufOffset := 0
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(pqm.CreatedTs))

	bufOffset += 8
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(pqm.PopCount))

	bufOffset += 8
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(pqm.UnlockTs))

	bufOffset += 8
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(pqm.DeliveryTs))

	bufOffset += 8
	buf[bufOffset] = pqm.pushAt // uint8

	return buf
}

func (pqm *DSQMessage) GetStatus() map[string]interface{} {
	res := make(map[string]interface{})
	res["CreatedTs"] = pqm.CreatedTs
	res["DeliveryTs"] = pqm.DeliveryTs
	res["pushAt"] = pqm.pushAt
	res["PopCount"] = pqm.PopCount
	res["UnlockTs"] = pqm.UnlockTs
	return res
}

var _ common.IItemMetaData = &DSQMessage{}
