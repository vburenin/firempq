package pmsg

import (
	"bufio"

	"github.com/vburenin/firempq/export/encoding"
	"github.com/vburenin/firempq/export/proto"
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
	return string(proto.UintToHex(p.Serial)) + "-" + string(proto.UintToHex(uint64(p.PopCount)))
}

func (p *FullMessage) WriteResponse(buf *bufio.Writer) (err error) {
	v := 5
	if p.UnlockTs > 0 {
		v = 6
	}

	encoding.WriteMapSize(buf, v)
	buf.WriteString(" ID ")
	encoding.WriteString(buf, p.StrId)

	buf.WriteString(" PL ")
	encoding.WriteBytes(buf, p.Payload)

	buf.WriteString(" ETS ")
	encoding.WriteInt64(buf, p.ExpireTs)

	buf.WriteString(" POPCNT ")
	encoding.WriteInt64(buf, p.PopCount)

	buf.WriteString(" UTS ")
	err = encoding.WriteInt64(buf, p.UnlockTs)

	if p.UnlockTs > 0 {
		buf.WriteString(" RCPT ")
		buf.Write(proto.UintToHex(p.Serial))
		buf.WriteByte('-')
		_, err = buf.Write(proto.UintToHex(uint64(p.PopCount)))
	}
	return err
}
