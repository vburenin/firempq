package pqueue

import (
	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/pmsg"
)

const DBActionAddMetadata = byte(1)
const DBActionUpdateMetadata = byte(2)
const DBActionDeleteMetadata = byte(3)
const DBActionWipeAll = byte(4)
const DBActionQueueRemoved = byte(5)
const DBActionNewConfig = byte(6)
const DBActionWrongData = byte(7)

type MetaActionDB struct {
	db         apis.DataStorage
	binQueueId []byte
}

func NewDBFunctor(queueID uint64, db apis.DataStorage) *MetaActionDB {
	b := make([]byte, 8)
	enc.Uint64ToBin(queueID, b)
	return &MetaActionDB{
		db:         db,
		binQueueId: b,
	}
}

func (dbf *MetaActionDB) UpdateMetadata(m *pmsg.MsgMeta) {
	data := make([]byte, m.Size()+9)
	copy(data, dbf.binQueueId)
	n, _ := m.MarshalTo(data[9:])
	data[8] = DBActionUpdateMetadata
	dbf.db.AddMetadata(data[:n+9])
}

func (dbf *MetaActionDB) DeleteMetadata(sn uint64) {
	data := make([]byte, 8+1+8)
	copy(data, dbf.binQueueId)
	data[8] = DBActionDeleteMetadata
	enc.Uint64ToBin(sn, data[9:])
	dbf.db.AddMetadata(data)
}

func (dbf *MetaActionDB) WipeAll() {
	data := make([]byte, 8+1)
	copy(data, dbf.binQueueId)
	data[9] = DBActionWipeAll
	dbf.db.AddMetadata(data)
}

func (dbf *MetaActionDB) AddMetadata(m *pmsg.MsgMeta) {
	data := make([]byte, m.Size()+9)
	copy(data, dbf.binQueueId)
	n, _ := m.MarshalTo(data[9:])
	data[8] = DBActionAddMetadata
	final := data[:n+9]
	dbf.db.AddMetadata(final)
}
