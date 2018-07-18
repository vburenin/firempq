package pqueue

import "github.com/vburenin/firempq/pmsg"

type MessagePool struct {
	msgPool []*pmsg.MsgMeta
	poolPos int
}

func (mp *MessagePool) InitPool(size int) {
	mp.msgPool = make([]*pmsg.MsgMeta, size)
}

func (mp *MessagePool) AllocateNewMessage() *pmsg.MsgMeta {
	if mp.poolPos > 0 {
		m := mp.msgPool[mp.poolPos-1]
		mp.poolPos--
		return m
	}
	return &pmsg.MsgMeta{}
}

func (mp *MessagePool) ReturnMessage(m *pmsg.MsgMeta) {
	m.Reset()
	if mp.poolPos == len(mp.msgPool) {
		return
	} else {
		mp.msgPool[mp.poolPos] = m
		mp.poolPos++
	}
}
