package pmsg

import (
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/log"
)

func NewPMsgMeta(id string, priority int64, expireTs int64, sn uint64) *PMsgMeta {
	return &PMsgMeta{
		Serial:   sn,
		Priority: priority,
		ExpireTs: expireTs,
		PopCount: 0,
		UnlockTs: 0,
		StrId:    id,
	}
}

func (m *PMsgMeta) Sn2Bin() string {
	return enc.Sn2Bin(m.Serial)
}

func DecodePMsgMeta(sn uint64, buf []byte) *PMsgMeta {
	p := PMsgMeta{Serial: sn}
	if err := p.Unmarshal(buf); err != nil {
		log.Error("Could not unmarshal message: %d", sn)
		return nil
	}
	return &p
}

func (self *PMsgMeta) ByteMarshal() []byte {
	data, _ := self.Marshal()
	return data
}
