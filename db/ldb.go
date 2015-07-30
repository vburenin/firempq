package db

// Level DB wrapper over multiple queue data store.
// Each key has a prefix that combines queue name as well as type of stored data.
// The following format is used for item keys:
// somename:m:itemid
// This format is used for payload keys:
// somename:p:payload

import (
	"bytes"
	"firempq/common"
	"firempq/qerrors"
	"firempq/util"
	"github.com/jmhodges/levigo"
	"github.com/op/go-logging"
	"sync"
	"time"
)

var log = logging.MustGetLogger("ldb")

// Item iterator built on top of LevelDB.
// It takes into account queue name to limit the amount of selected data.
type ItemIterator struct {
	iter   *levigo.Iterator
	prefix []byte // Prefix for look ups.
	Key    []byte // Currently selected key. Valid only if the iterator is valid.
	Value  []byte // Currently selected value. Valid only if the iterator is valid.
}

func NewIter(iter *levigo.Iterator, prefix []byte) *ItemIterator {
	iter.Seek(prefix)
	return &ItemIterator{iter, prefix, nil, nil}
}

// Switch to the next element.
func (mi *ItemIterator) Next() {
	mi.iter.Next()
}

// Before value is read, check if iterator is valid.
// for iter.Valid() {
//    mykey := iter.Key
//    myvalue := iter.Value
//    ......
//    iter.Next()
//}
func (mi *ItemIterator) Valid() bool {
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
func (mi *ItemIterator) Close() {
	mi.iter.Close()
}

type DataStorage struct {
	db              *levigo.DB        // Pointer the the instance of level db.
	dbName          string            // LevelDB database name.
	itemCache       map[string][]byte // Active cache for item metadata.
	payloadCache    map[string]string // Active cache for item payload.
	tmpItemCache    map[string][]byte // Temporary map of cached data while it is in process of flushing into DB.
	tmpPayloadCache map[string]string // Temporary map of cached payloads while it is in process of flushing into DB.
	cacheLock       sync.Mutex        // Used for caches access.
	flushLock       sync.Mutex        // Used to prevent double flush.
	closed          bool
}

func NewDataStorage(dbName string) *DataStorage {
	ds := DataStorage{
		dbName:          dbName,
		itemCache:       make(map[string][]byte),
		payloadCache:    make(map[string]string),
		tmpItemCache:    make(map[string][]byte),
		tmpPayloadCache: make(map[string]string),
		closed:          false,
	}
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(10 * 1024 * 1024)
	opts.SetCompression(levigo.SnappyCompression)
	db, err := levigo.Open(dbName, opts)
	if err != nil {
		log.Critical("Could not initialize database: %s", err.Error())
		return nil
	}
	ds.db = db
	go ds.periodicCacheFlush()
	return &ds
}

func (ds *DataStorage) periodicCacheFlush() {
	for !ds.closed {
		ds.flushLock.Lock()
		if !ds.closed {
			ds.FlushCache()
		}
		ds.flushLock.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// Item payload Id.
func makePayloadId(queueName, id string) string {
	return "p:" + queueName + ":" + id
}

// Item Id.
func makeItemId(queueName, id string) string {
	return "m:" + queueName + ":" + id
}

// Item will be stored into cache including payload.
func (ds *DataStorage) StoreItemWithPayload(queueName string, item common.IItemMetaData, payload string) {
	itemId := makeItemId(queueName, item.GetId())
	payloadId := makePayloadId(queueName, item.GetId())

	itemBody := item.ToBinary()

	ds.cacheLock.Lock()
	ds.itemCache[itemId] = itemBody
	ds.payloadCache[payloadId] = payload
	ds.cacheLock.Unlock()
}

func (ds *DataStorage) StoreItem(queueName string, item common.IItemMetaData) {
	itemId := makeItemId(queueName, item.GetId())

	itemBody := item.ToBinary()

	ds.cacheLock.Lock()
	ds.itemCache[itemId] = itemBody
	ds.cacheLock.Unlock()
}

// Updates item metadata, affects cache only until flushed.
func (ds *DataStorage) UpdateItem(queueName string, item common.IItemMetaData) {
	itemId := makeItemId(queueName, item.GetId())
	itemBody := item.ToBinary()
	ds.cacheLock.Lock()
	ds.itemCache[itemId] = itemBody
	ds.cacheLock.Unlock()
}

// Deletes item metadata and payload, affects cache only until flushed.
func (ds *DataStorage) DeleteItem(queueName string, itemId string) {
	id := makeItemId(queueName, itemId)
	payloadId := makePayloadId(queueName, itemId)
	ds.cacheLock.Lock()
	ds.itemCache[id] = nil
	ds.payloadCache[payloadId] = ""
	ds.cacheLock.Unlock()
}

// Returns item payload. Three places are checked:
// 1. Top level cache.
// 2. Temp cache while data is getting flushed into db.
// 3. If not found in cache, will do actually DB lookup.
func (ds *DataStorage) GetPayload(queueName string, itemId string) string {
	payloadId := makePayloadId(queueName, itemId)
	ds.cacheLock.Lock()
	payload, ok := ds.payloadCache[itemId]
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
	if ds.closed {
		return
	}
	ds.cacheLock.Lock()
	ds.tmpItemCache = ds.itemCache
	ds.tmpPayloadCache = ds.payloadCache
	ds.itemCache = make(map[string][]byte)
	ds.payloadCache = make(map[string]string)
	ds.cacheLock.Unlock()

	wb := levigo.NewWriteBatch()
	for k, v := range ds.tmpItemCache {
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
	ds.db.Write(wopts, wb)

	ds.cacheLock.Lock()
	ds.tmpItemCache = make(map[string][]byte)
	ds.tmpPayloadCache = make(map[string]string)
	ds.cacheLock.Unlock()
}

// Returns new Iterator that must be closed!
// Queue name used as a prefix to iterate over all item metadata that belongs to the specific queue.
func (ds *DataStorage) IterQueue(queueName string) *ItemIterator {
	ropts := levigo.NewReadOptions()
	iter := ds.db.NewIterator(ropts)
	prefix := []byte(makeItemId(queueName, ""))
	return NewIter(iter, prefix)
}

const QUEUE_META_PREFIX = "qmeta:"
const QUEUE_SETTINGS_PREFIX = "qsettings:"

// Read all available queues.
func (ds *DataStorage) GetAllQueueMeta() []*common.QueueMetaInfo {
	qmiList := make([]*common.QueueMetaInfo, 0, 1000)
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
			log.Error("Coudn't read queue meta data because of: %s", err.Error())
			iter.Next()
			continue
		}
		if qmi.Disabled {
			log.Error("Qeueue is disabled. Skipping: %s", qmi.Name)
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

func (ds *DataStorage) SaveQueueSettings(queueName string, settings interface{}) {
	key := makeSettingsKey(queueName)
	wopts := levigo.NewWriteOptions()
	data := util.StructToBinary(settings)
	err := ds.db.Put(wopts, key, data)
	if err != nil {
		log.Error("Failed to save settings: %s", err.Error())
	}
}

// Flush and close database.
func (ds *DataStorage) Close() {
	ds.flushLock.Lock()
	defer ds.flushLock.Unlock()
	if !ds.closed {
		ds.closed = true
		ds.FlushCache()
		ds.db.CompactRange(levigo.Range{nil, nil})
		ds.db.Close()
	} else {
		log.Error("Attempt to close database more than once!")
	}
}
