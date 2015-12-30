package pqmsg

import (
	"bytes"
	. "firempq/common"
	. "firempq/common/response_encoder"
)

type MsgResponseItem struct {
	msgId    string
	payload  string
	expireTs int64
	popCount int64
	unlockTs int64
}

func NewMsgResponseItem(id string, payload string, expireTs, popCount, unlockTs int64) *MsgResponseItem {
	return &MsgResponseItem{
		msgId:    id,
		payload:  payload,
		expireTs: expireTs,
		popCount: popCount,
		unlockTs: unlockTs,
	}
}

func (p *MsgResponseItem) GetId() string {
	return p.msgId
}

func (p *MsgResponseItem) GetPayload() string {
	return p.payload
}

func (p *MsgResponseItem) Encode() string {
	var buf bytes.Buffer
	buf.WriteString(EncodeMapSize(5))

	buf.WriteString(" ID")
	buf.WriteString(EncodeString(p.msgId))

	buf.WriteString(" PL")
	buf.WriteString(EncodeString(p.payload))

	buf.WriteString(" ETS")
	buf.WriteString(EncodeInt64(p.expireTs))

	buf.WriteString(" POPCNT")
	buf.WriteString(EncodeInt64(p.popCount))

	buf.WriteString(" UTS")
	buf.WriteString(EncodeInt64(p.unlockTs))
	return UnsafeBytesToString(buf.Bytes())
}
