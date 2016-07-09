package pqueue

import (
	"bytes"

	"github.com/vburenin/firempq/enc"
)

type MsgResponseItem struct {
	msg     *PQMsgMetaData
	payload []byte
}

func NewMsgResponseItem(msg *PQMsgMetaData, payload []byte) *MsgResponseItem {
	return &MsgResponseItem{
		msg:     msg,
		payload: payload,
	}
}

func (p *MsgResponseItem) GetId() string {
	return p.msg.StrId
}

func (p *MsgResponseItem) GetPayload() []byte {
	return p.payload
}

func (p *MsgResponseItem) GetReceipt() string {
	return enc.EncodeTo36Base(p.msg.SerialNumber) + "-" + enc.EncodeTo36Base(uint64(p.msg.PopCount))
}

func (p *MsgResponseItem) GetMeta() *PQMsgMetaData {
	return p.msg
}

func (p *MsgResponseItem) WriteResponse(buf *bytes.Buffer) error {

	v := 5
	if p.msg.UnlockTs > 0 {
		v = 6
	}

	err := enc.WriteDictSize(buf, v)
	_, err = buf.WriteString(" ID ")
	err = enc.WriteString(buf, p.msg.StrId)

	_, err = buf.WriteString(" PL ")
	err = enc.WriteBytes(buf, p.payload)

	_, err = buf.WriteString(" ETS ")
	err = enc.WriteInt64(buf, p.msg.ExpireTs)

	_, err = buf.WriteString(" POPCNT ")
	err = enc.WriteInt64(buf, p.msg.PopCount)

	_, err = buf.WriteString(" UTS ")
	err = enc.WriteInt64(buf, p.msg.UnlockTs)

	if p.msg.UnlockTs > 0 {
		_, err = buf.WriteString(" RCPT ")
		_, err = buf.WriteString(enc.EncodeTo36Base(p.msg.SerialNumber))
		err = buf.WriteByte('-')
		_, err = buf.WriteString(enc.EncodeTo36Base(uint64(p.msg.PopCount)))
	}
	return err
}
