package dsqueue

import (
	"encoding/binary"
	"firempq/common"
)

const (
	MAX_MESSAGE_ID_LENGTH = 64
)

type DSQMessage struct {
	Id         string
	CreatedTs  int64
	PopCount   int64
	UnlockTs   int64
	DeliveryTs int64
	pushAt     uint8
	ListId     int64
}

func NewDSQMessageWithId(id string) *DSQMessage {

	m := DSQMessage{
		Id:         id,
		CreatedTs:  common.Uts(),
		PopCount:   0,
		UnlockTs:   0,
		DeliveryTs: 0,
		ListId:     0,
	}
	return &m
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
	listID := binary.BigEndian.Uint64(buf[bufOffset:])

	bufOffset += 8
	pushAt := uint8(buf[bufOffset])

	return &DSQMessage{
		Id:         msgId,
		CreatedTs:  int64(createdTs),
		PopCount:   int64(popCount),
		UnlockTs:   int64(unlockTs),
		DeliveryTs: int64(deliveryTs),
		pushAt:     uint8(pushAt),
		ListId:     int64(listID),
	}
}

func (m *DSQMessage) GetId() string {
	return m.Id
}

func (m *DSQMessage) ToBinary() []byte {
	// length of 5 64 bits integers and one 8 bit.

	buf := make([]byte, 8*6)

	bufOffset := 0
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(m.CreatedTs))

	bufOffset += 8
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(m.PopCount))

	bufOffset += 8
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(m.UnlockTs))

	bufOffset += 8
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(m.DeliveryTs))

	bufOffset += 8
	binary.BigEndian.PutUint64(buf[bufOffset:], uint64(m.ListId))

	bufOffset += 8
	buf[bufOffset] = m.pushAt

	return buf
}

func (m *DSQMessage) GetStatus() map[string]interface{} {
	res := make(map[string]interface{})
	res["CreatedTs"] = m.CreatedTs
	res["DeliveryTs"] = m.DeliveryTs
	res["pushAt"] = m.pushAt
	res["PopCount"] = m.PopCount
	res["UnlockTs"] = m.UnlockTs
	res["ListID"] = m.ListId
	return res
}

var _ common.IItemMetaData = &DSQMessage{}
