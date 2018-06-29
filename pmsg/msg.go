package pmsg

func NewPMsgMeta(payloadFileID, payloadOffset int64, id string, expireTs int64, sn uint64) *MsgMeta {
	return &MsgMeta{
		Serial:        sn,
		ExpireTs:      expireTs,
		PopCount:      0,
		UnlockTs:      0,
		StrId:         id,
		PayloadFileId: payloadFileID,
		PayloadOffset: payloadOffset,
	}
}
