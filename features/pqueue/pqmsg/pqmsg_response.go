package pqmsg

import (
	"bytes"
	. "firempq/common"
	. "firempq/common/response_encoder"
)

type MsgResponseItem struct {
	msg     *PQMsgMetaData
	payload string
}

func NewMsgResponseItem(msg *PQMsgMetaData, payload string) *MsgResponseItem {
	return &MsgResponseItem{
		msg:     msg,
		payload: payload,
	}
}

func (p *MsgResponseItem) GetId() string {
	return p.msg.StrId
}

func (p *MsgResponseItem) GetPayload() string {
	return p.payload
}

func (p *MsgResponseItem) GetReceipt() string {
	return EncodeTo36Base(p.msg.SerialNumber) + "-" + EncodeTo36Base(uint64(p.msg.PopCount))
}

func (p *MsgResponseItem) Encode() string {
	var buf bytes.Buffer
	buf.WriteString(EncodeMapSize(6))

	buf.WriteString(" ID")
	buf.WriteString(EncodeString(p.msg.StrId))

	buf.WriteString(" PL")
	buf.WriteString(EncodeString(p.payload))

	buf.WriteString(" ETS")
	buf.WriteString(EncodeInt64(p.msg.ExpireTs))

	buf.WriteString(" POPCNT")
	buf.WriteString(EncodeInt64(p.msg.PopCount))

	buf.WriteString(" UTS")
	buf.WriteString(EncodeInt64(p.msg.UnlockTs))

	if p.msg.UnlockTs > 0 {
		buf.WriteString(" RCPT ")
		buf.WriteString(EncodeTo36Base(p.msg.SerialNumber))
		buf.WriteString("-")
		buf.WriteString(EncodeTo36Base(uint64(p.msg.PopCount)))
	}
	return UnsafeBytesToString(buf.Bytes())
}
