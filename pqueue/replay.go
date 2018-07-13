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
	msgs    map[uint64]*pmsg.MsgMeta
	msgPool []*pmsg.MsgMeta
	poolPos int
}

func NewQueueLoader() *QueueLoader {
	return &QueueLoader{
		msgs:    make(map[uint64]*pmsg.MsgMeta),
		msgPool: make([]*pmsg.MsgMeta, 0, 100),
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

func (pql *QueueLoader) ReplayData(ctx *fctx.Context, iter apis.ItemIterator) error {
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

		data := iter.GetData()
		action := data[0]
		data = data[1:]

		switch action {
		case DBActionAddMetadata, DBActionUpdateMetadata:
			msg := pql.getNewMsg()
			if err := msg.Unmarshal(data); err != nil {
				ctx.Errorf("wrong data from iterator: %s", err)
			} else {
				pql.msgs[msg.Serial] = msg
			}
		case DBActionDeleteMetadata:
			if len(data) < 8 {
				ctx.Errorf("Invalid length of 'delete' data")
			} else {
				delSn := enc.DecodeBytesToUnit64(data)
				m := pql.msgs[delSn]
				if m != nil {
					pql.retMsg(m)
				}
			}
		case DBActionWipeAll:
			pql.msgs = make(map[uint64]*pmsg.MsgMeta, 4096)
		default:
			ctx.Errorf("Unknown action: %d", action)
		}
	}
}

func (pql *QueueLoader) getNewMsg() *pmsg.MsgMeta {
	if pql.poolPos > 0 {
		m := pql.msgPool[pql.poolPos-1]
		pql.poolPos--
		return m
	}
	return &pmsg.MsgMeta{}
}

func (pql *QueueLoader) retMsg(m *pmsg.MsgMeta) {
	m.Reset()
	if pql.poolPos == len(pql.msgPool) {
		pql.msgPool = append(pql.msgPool, m)
		pql.poolPos++
	} else {
		pql.msgPool[pql.poolPos] = m
		pql.poolPos++
	}
}
