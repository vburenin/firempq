package pqmsg

import (
	"firempq/log"

	. "firempq/utils"
	"firempq/encoding"
)

type PQMsgMetaData struct {
	SerialNumber uint64
	PQueueMsgData
}

func NewPQMsgMetaData(id string, priority int64, expireTs int64, sn uint64) *PQMsgMetaData {
	return &PQMsgMetaData{
		SerialNumber: sn,
		PQueueMsgData: PQueueMsgData{
			Priority: priority,
			ExpireTs: expireTs,
			PopCount: 0,
			UnlockTs: 0,
			StrId:    id,
		},
	}
}

func (pqm *PQMsgMetaData) Sn2Bin() string {
	return encoding.Sn2Bin(pqm.SerialNumber)
}

func UnmarshalPQMsgMetaData(sn uint64, buf []byte) *PQMsgMetaData {
	p := PQMsgMetaData{SerialNumber: sn}
	if err := p.Unmarshal(buf); err != nil {
		log.Error("Could not unmarshal message: %d", sn)
		return nil
	}
	return &p
}

func (self *PQMsgMetaData) StringMarshal() string {
	data, _ := self.Marshal()
	return UnsafeBytesToString(data)
}
