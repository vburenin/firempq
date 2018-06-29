package pqueue

import "github.com/vburenin/firempq/pmsg"

type MessageQueue struct {
	mainLine  *pmsg.MessageArrayList
	frontLine *pmsg.MessageArrayList
}

const rowSize = 4096

func NewMsgQueue() *MessageQueue {
	return &MessageQueue{
		mainLine:  pmsg.NewMessageArrayList(rowSize),
		frontLine: pmsg.NewMessageArrayList(rowSize),
	}
}

func (mq *MessageQueue) Clear() {
	mq.mainLine = pmsg.NewMessageArrayList(rowSize)
	mq.frontLine = pmsg.NewMessageArrayList(rowSize)
}

func (mq *MessageQueue) Add(msg *pmsg.MsgMeta) {
	mq.mainLine.Add(msg)
}

func (mq *MessageQueue) Return(msg *pmsg.MsgMeta) {
	mq.frontLine.Add(msg)
}

func (mq *MessageQueue) Size() uint64 {
	return mq.mainLine.Size() + mq.frontLine.Size()
}

func (mq *MessageQueue) Empty() bool {
	return mq.mainLine.Size() == 0 && mq.frontLine.Size() == 0
}

func (mq *MessageQueue) Pop() *pmsg.MsgMeta {
	m := mq.frontLine.Pop()
	if m != nil {
		return m
	}
	return mq.mainLine.Pop()
}
