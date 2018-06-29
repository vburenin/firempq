package pqueue

import (
	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/pmsg"
)

const DBActionAddMetadata = byte(0)
const DBActionUpdateMetadata = byte(1)
const DBActionDeleteMetadata = byte(2)
const DBActionWipeAll = byte(3)
const DBActionQueueRemoved = byte(4)
const DBActionNewConfig = byte(5)
const DBActionWrongData = byte(6)

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
	data[9] = DBActionUpdateMetadata
	dbf.db.AddMetadata(data[:n+9])
}

func (dbf *MetaActionDB) DeleteMetadata(sn uint64) {
	data := make([]byte, 8+1+8)
	copy(data, dbf.binQueueId)
	data[9] = DBActionDeleteMetadata
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
	data[9] = DBActionAddMetadata
	dbf.db.AddMetadata(data[:n+9])
}

func DecodeMetadata(data []byte) (action byte, queueID uint64, msg *pmsg.MsgMeta) {
	if len(data) < 9 {
		return DBActionWrongData, 0, nil
	}

	queueID = enc.DecodeBytesToUnit64(data)
	action = data[8]

	switch action {
	case DBActionAddMetadata, DBActionUpdateMetadata:
		msg = &pmsg.MsgMeta{}
		if err := msg.Unmarshal(data[9:]); err != nil {
			return DBActionWrongData, 0, nil
		}
	case DBActionDeleteMetadata:
		if len(data) < 17 {
			return DBActionWrongData, 0, nil
		}
		msg = &pmsg.MsgMeta{Serial: enc.DecodeBytesToUnit64(data[9:])}
	case DBActionQueueRemoved:
	case DBActionWipeAll:
	default:
		return DBActionWrongData, 0, nil
	}
	return action, queueID, msg
}
