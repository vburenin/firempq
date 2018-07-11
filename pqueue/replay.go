package pqueue

import (
	"sort"

	"io"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/ferr"
	"github.com/vburenin/firempq/pmsg"
)

type MsgArray []*pmsg.MsgMeta

func (m MsgArray) Len() int           { return len(m) }
func (m MsgArray) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m MsgArray) Less(i, j int) bool { return m[i].Serial < m[j].Serial }

type QueueLoader struct {
	msgs map[uint64]*pmsg.MsgMeta
}

func NewQueueLoader() *QueueLoader {
	return &QueueLoader{
		msgs: make(map[uint64]*pmsg.MsgMeta),
	}
}

func (pql *QueueLoader) Messages() MsgArray {
	output := make(MsgArray, 0, len(pql.msgs))
	for _, m := range pql.msgs {
		output = append(output, m)
	}
	pql.msgs = nil
	sort.Sort(output)
	return output
}

var msgPool = make([]*pmsg.MsgMeta, 0, 100)
var poolPos = 0

func getNewMsg() *pmsg.MsgMeta {
	if poolPos > 0 {
		m := msgPool[poolPos-1]
		poolPos--
		return m
	}
	return &pmsg.MsgMeta{}
}

func retMsg(m *pmsg.MsgMeta) {
	m.Reset()
	if poolPos == len(msgPool) {
		msgPool = append(msgPool, m)
		poolPos++
	} else {
		msgPool[poolPos] = m
		poolPos++
	}
}

func ReplayData(ctx *fctx.Context, queues map[uint64]*QueueLoader, iter apis.ItemIterator) error {
	for {
		if err := iter.Next(); err != nil {
			if err == io.EOF {
				return nil
			}
			return ferr.Wrapf(err, "failed to complete iteration over data")
		}
		if !iter.Valid() {
			continue
		}

		action, queueID, msg, delSn := DecodeMetadata(iter.GetData())
		if action == DBActionWrongData {
			ctx.Error("Received blob cannot be decoded")
		}

		loader := queues[queueID]
		if loader == nil {
			continue
		}

		switch action {
		case DBActionAddMetadata, DBActionUpdateMetadata:
			loader.msgs[msg.Serial] = msg
		case DBActionDeleteMetadata:
			m := loader.msgs[delSn]
			if m != nil {
				retMsg(m)
			}
			delete(loader.msgs, delSn)
		case DBActionWipeAll:
			loader.msgs = make(map[uint64]*pmsg.MsgMeta, 4096)
		}

	}
}

func DecodeMetadata(data []byte) (action byte, queueID uint64, msg *pmsg.MsgMeta, msgSn uint64) {
	if len(data) < 9 {
		println("too short data")
		return DBActionWrongData, 0, nil, 0
	}

	queueID = enc.DecodeBytesToUnit64(data)
	action = data[8]
	data = data[9:]

	switch action {
	case DBActionAddMetadata, DBActionUpdateMetadata:
		msg := getNewMsg()
		if err := msg.Unmarshal(data); err != nil {
			return DBActionWrongData, 0, nil, 0
		}
		return action, queueID, msg, 0
	case DBActionDeleteMetadata:
		if len(data) < 8 {
			return DBActionWrongData, 0, nil, 0
		}
		return action, queueID, nil, enc.DecodeBytesToUnit64(data)
	case DBActionQueueRemoved:
	case DBActionWipeAll:
	default:
		return DBActionWrongData, 0, nil, 0
	}
	return action, queueID, msg, 0
}
