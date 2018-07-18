package pmsg

import (
	"bufio"

	"github.com/vburenin/firempq/enc"
)

type FullMessage struct {
	MsgMeta
	Payload []byte
}

func NewFullMessage(msg *MsgMeta, payload []byte) *FullMessage {
	return &FullMessage{
		MsgMeta: MsgMeta(*msg),
		Payload: payload,
	}
}

func (p *FullMessage) Receipt() string {
	return enc.To36Base(p.Serial) + "-" + enc.To36Base(uint64(p.PopCount))
}

func (p *FullMessage) WriteResponse(buf *bufio.Writer) error {

	v := 5
	if p.UnlockTs > 0 {
		v = 6
	}

	err := enc.WriteDictSize(buf, v)
	_, err = buf.WriteString(" ID ")
	err = enc.WriteString(buf, p.StrId)

	_, err = buf.WriteString(" PL ")
	err = enc.WriteBytes(buf, p.Payload)

	_, err = buf.WriteString(" ETS ")
	err = enc.WriteInt64(buf, p.ExpireTs)

	_, err = buf.WriteString(" POPCNT ")
	err = enc.WriteInt64(buf, p.PopCount)

	_, err = buf.WriteString(" UTS ")
	err = enc.WriteInt64(buf, p.UnlockTs)

	if p.UnlockTs > 0 {
		_, err = buf.WriteString(" RCPT ")
		_, err = buf.WriteString(enc.To36Base(p.Serial))
		err = buf.WriteByte('-')
		_, err = buf.WriteString(enc.To36Base(uint64(p.PopCount)))
	}
	return err
}
