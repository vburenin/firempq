package pqueue

import (
	"bufio"

	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/pmsg"
)

type MsgResponseItem struct {
	msg     *pmsg.PMsgMeta
	payload []byte
}

func NewMsgResponseItem(msg *pmsg.PMsgMeta, payload []byte) *MsgResponseItem {
	return &MsgResponseItem{
		msg:     msg,
		payload: payload,
	}
}

func (p *MsgResponseItem) ID() string {
	return p.msg.StrId
}

func (p *MsgResponseItem) Payload() []byte {
	return p.payload
}

func (p *MsgResponseItem) Receipt() string {
	return enc.To36Base(p.msg.Serial) + "-" + enc.To36Base(uint64(p.msg.PopCount))
}

func (p *MsgResponseItem) GetMeta() *pmsg.PMsgMeta {
	return p.msg
}

func (p *MsgResponseItem) WriteResponse(buf *bufio.Writer) error {

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
		_, err = buf.WriteString(enc.To36Base(p.msg.Serial))
		err = buf.WriteByte('-')
		_, err = buf.WriteString(enc.To36Base(uint64(p.msg.PopCount)))
	}
	return err
}
