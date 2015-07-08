package db

// Level DB wrapper over multiple queue data store.
// Each key has a prefix that combines queue name as well as type of stored data.
// The following format is used for message keys:
// somename:m:msgid
// This format is used for payload keys:
// somename:p:payload

import (
	"bytes"
	"firempq/common"
	"firempq/qerrors"
	"firempq/queue_facade"
	"firempq/util"
	"github.com/jmhodges/levigo"
	"log"
	"sync"
)

// Message iterator built on top of LevelDB. It takes into account queue name to limit the amount of selected data.
type MsgIterator struct {
	iter   *levigo.Iterator
	prefix []byte // Prefix for look ups.
	Key    []byte // Currently selected key. Valid only if the iterator is valid.
	Value  []byte // Currently selected value. Valid only if the iterator is valid.
}

func NewIter(iter *levigo.Iterator, prefix []byte) *MsgIterator {
	iter.Seek(prefix)
	return &MsgIterator{iter, prefix, nil, nil}
}

// Switch to the next element.
func (mi *MsgIterator) Next() {
	mi.iter.Next()
}

// Before value is read, check if iterator is valid.
// for iter.Valid() {
//    mykey := iter.Key
//    myvalue := iter.Value
//    ......
//    iter.Next()
//}
func (mi *MsgIterator) Valid() bool {
	valid := mi.iter.Valid()
	if valid {
		k := mi.iter.Key()
		// Strip key prefix. If prefix doesn't match the length of the slice will remain the same.
		normKey := bytes.TrimPrefix(k, mi.prefix)
		if len(normKey) == len(k) {
			return false
		}
		mi.Key = normKey
		mi.Value = mi.iter.Value()
		return true
	}
	return false
}

// Iterator must be closed!
func (mi *MsgIterator) Close() {
	mi.iter.Close()
}

type DataStorage struct {
	db              *levigo.DB        // Pointer the the instance of level db.
	dbName          string            // LevelDB database name.
	msgCache        map[string][]byte // Active cache for message metadata.
	payloadCache    map[string]string // Active cache for message payload.
	tmpMsgCache     map[string][]byte // Temporary map of cached data while it is in process of flushing into DB.
	tmpPayloadCache map[string]string // Temporary map of cached payloads while it is in process of flushing into DB.
	cacheLock       sync.Mutex        // Used for caches access.
	flushLock       sync.Mutex        // Used to prevent double flush.
}

func NewDataStorage(dbName string) *DataStorage {
	ds := DataStorage{
		dbName:          dbName,
		msgCache:        make(map[string][]byte),
		payloadCache:    make(map[string]string),
		tmpMsgCache:     make(map[string][]byte),
		tmpPayloadCache: make(map[string]string),
	}
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(10 * 1024 * 1024)
	opts.SetCompression(levigo.SnappyCompression)
	db, err := levigo.Open(dbName, opts)
	if err != nil {
		log.Fatalln("Could not initialize database: ", err)
	}
	ds.db = db
	return &ds
}

// Message payload Id.
func makePayloadId(queueName, msgId string) string {
	return "p:" + queueName + ":" + msgId
}

// Message Id.
func makeMsgId(queueName, msgId string) string {
	return "m:" + queueName + ":" + msgId
}

// Message will be stored into cache including payload.
func (ds *DataStorage) StoreMessage(queueName string, msg queue_facade.IMessage, payload string) {
	itemId := makeMsgId(queueName, msg.GetId())
	payloadId := makePayloadId(queueName, msg.GetId())

	msgBody := msg.ToBinary()

	ds.cacheLock.Lock()
	ds.msgCache[itemId] = msgBody
	ds.payloadCache[payloadId] = payload
	ds.cacheLock.Unlock()
}

// Updates message metadata, affects cache only until flushed.
func (ds *DataStorage) UpdateMessage(queueName string, msg queue_facade.IMessage) {
	itemId := makeMsgId(queueName, msg.GetId())
	msgBody := msg.ToBinary()
	ds.cacheLock.Lock()
	ds.msgCache[itemId] = msgBody
	ds.cacheLock.Unlock()
}

// Deletes message metadata and payload, affects cache only until flushed.
func (ds *DataStorage) DeleteMessage(queueName string, msgId string) {
	itemId := makeMsgId(queueName, msgId)
	payloadId := makePayloadId(queueName, msgId)
	ds.cacheLock.Lock()
	ds.msgCache[itemId] = nil
	ds.payloadCache[payloadId] = ""
	ds.cacheLock.Unlock()
}

// Returns message payload. Three places are checked:
// 1. Top level cache.
// 2. Temp cache while data is getting flushed into db.
// 3. If not found in cache, will do actually DB lookup.
func (ds *DataStorage) GetPayload(queueName string, msgId string) string {
	payloadId := makePayloadId(queueName, msgId)
	ds.cacheLock.Lock()
	payload, ok := ds.payloadCache[msgId]
	if ok {
		ds.cacheLock.Unlock()
		return payload
	}
	payload, ok = ds.tmpPayloadCache[payloadId]
	if ok {
		ds.cacheLock.Unlock()
		return payload
	}
	ds.cacheLock.Unlock()
	ropts := levigo.NewReadOptions()
	value, _ := ds.db.Get(ropts, []byte(payloadId))
	return string(value)
}

func (ds *DataStorage) FlushCache() {
	ds.cacheLock.Lock()
	ds.tmpMsgCache = ds.msgCache
	ds.tmpPayloadCache = ds.payloadCache
	ds.msgCache = make(map[string][]byte)
	ds.payloadCache = make(map[string]string)
	ds.cacheLock.Unlock()

	wb := levigo.NewWriteBatch()
	for k, v := range ds.tmpMsgCache {
		key := []byte(k)
		if v == nil {
			wb.Delete(key)
		} else {
			wb.Put(key, v)
		}
	}

	for k, v := range ds.tmpPayloadCache {
		key := []byte(k)
		if v == "" {
			wb.Delete(key)
		} else {
			wb.Put(key, []byte(v))
		}
	}

	wopts := levigo.NewWriteOptions()
	ds.flushLock.Lock()
	ds.db.Write(wopts, wb)
	ds.flushLock.Unlock()

	ds.cacheLock.Lock()
	ds.tmpMsgCache = make(map[string][]byte)
	ds.tmpPayloadCache = make(map[string]string)
	ds.cacheLock.Unlock()
}

// Returns new Iterator that must be closed!
// Queue name used as a prefix to iterate over all message metadata that belongs to the specific queue.
func (ds *DataStorage) IterQueue(queueName string) *MsgIterator {
	ropts := levigo.NewReadOptions()
	iter := ds.db.NewIterator(ropts)
	prefix := []byte(makeMsgId(queueName, ""))
	return NewIter(iter, prefix)
}

const QUEUE_META_PREFIX = "qmeta:"
const QUEUE_SETTINGS_PREFIX = "qsettings:"

// Read all available queues.
func (ds *DataStorage) GetAllQueueMeta() []*common.QueueMetaInfo {
	qmiList := make([]*common.QueueMetaInfo, 1)
	qtPrefix := []byte(QUEUE_META_PREFIX)
	ropts := levigo.NewReadOptions()
	iter := ds.db.NewIterator(ropts)
	iter.Seek(qtPrefix)
	for iter.Valid() {
		queueKey := iter.Key()
		queueName := bytes.TrimPrefix(queueKey, qtPrefix)

		if len(queueKey) == len(queueName) {
			break
		}
		qmi, err := common.QueueInfoFromBinary(iter.Value())
		if err != nil {
			log.Println("Coudn't read queue meta data because of:", err.Error())
			iter.Next()
			continue
		}
		if qmi.Disabled {
			log.Println("Qeueue is disabled. Skipping:", qmi.Name)
			iter.Next()
			continue
		}
		qmiList = append(qmiList, qmi)
		iter.Next()
	}
	return qmiList
}

func (ds *DataStorage) SaveQueueMeta(qmi *common.QueueMetaInfo) {
	key := QUEUE_META_PREFIX + qmi.Name
	wopts := levigo.NewWriteOptions()
	ds.db.Put(wopts, []byte(key), qmi.ToBinary())
}

func makeSettingsKey(queueName string) []byte {
	return []byte(QUEUE_SETTINGS_PREFIX + queueName)
}

// Read queue settings bases on queue name. Caller should profide correct settings structure to read binary data.
func (ds *DataStorage) GetQueueSettings(settings interface{}, queueName string) error {
	key := makeSettingsKey(queueName)
	ropts := levigo.NewReadOptions()
	data, _ := ds.db.Get(ropts, key)
	if data == nil {
		return qerrors.InvalidRequest("No queue settings found: " + queueName)
	}
	err := util.StructFromBinary(settings, data)
	return err
}

func (ds *DataStorage) SaveQueueSettings(settings interface{}, queueName string) {
	key := makeSettingsKey(queueName)
	wopts := levigo.NewWriteOptions()
	data := util.StructToBinary(settings)
	err := ds.db.Put(wopts, key, data)
	if err != nil {
		log.Println("Failed to save settings: ", err.Error())
	}
}

// Flush and close database.
func (ds *DataStorage) Close() {
	ds.FlushCache()
	ds.db.Close()
}
