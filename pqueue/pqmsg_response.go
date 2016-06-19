package pqueue

import (
	"bytes"

	"github.com/vburenin/firempq/enc"
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
	return enc.EncodeTo36Base(p.msg.SerialNumber) + "-" + enc.EncodeTo36Base(uint64(p.msg.PopCount))
}

func (p *MsgResponseItem) GetMeta() *PQMsgMetaData {
	return p.msg
}

func (p *MsgResponseItem) Encode() string {
	var buf bytes.Buffer
	buf.WriteString(enc.EncodeMapSize(6))

	buf.WriteString(" ID")
	buf.WriteString(enc.EncodeString(p.msg.StrId))

	buf.WriteString(" PL")
	buf.WriteString(enc.EncodeString(p.payload))

	buf.WriteString(" ETS")
	buf.WriteString(enc.EncodeInt64(p.msg.ExpireTs))

	buf.WriteString(" POPCNT")
	buf.WriteString(enc.EncodeInt64(p.msg.PopCount))

	buf.WriteString(" UTS")
	buf.WriteString(enc.EncodeInt64(p.msg.UnlockTs))

	if p.msg.UnlockTs > 0 {
		buf.WriteString(" RCPT ")
		buf.WriteString(enc.EncodeTo36Base(p.msg.SerialNumber))
		buf.WriteString("-")
		buf.WriteString(enc.EncodeTo36Base(uint64(p.msg.PopCount)))
	}
	return enc.UnsafeBytesToString(buf.Bytes())
}
