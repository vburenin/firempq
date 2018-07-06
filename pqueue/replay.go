package pqueue

import (
	"sort"

	"io"

	"github.com/vburenin/firempq/apis"
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

func (pql *QueueLoader) Update(action byte, msg *pmsg.MsgMeta) {
	switch action {
	case DBActionAddMetadata, DBActionUpdateMetadata:
		pql.msgs[msg.Serial] = msg
	case DBActionDeleteMetadata:
		delete(pql.msgs, msg.Serial)
	case DBActionWipeAll:
		pql.msgs = make(map[uint64]*pmsg.MsgMeta, 4096)
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

		action, queueID, msg := DecodeMetadata(iter.GetData())
		if action == DBActionWrongData {
			ctx.Error("Received blob cannot be decoded")
		}
		loader := queues[queueID]
		if loader == nil {
			continue
		}
		loader.Update(action, msg)
	}
}
