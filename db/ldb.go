package db

// Level DB wrapper over multiple service data store.
// Each key has a prefix that combines service name as well as type of stored data.
// The following format is used for item keys:
// somename:m:itemid
// This format is used for payload keys:
// somename:p:payload

import (
	"bytes"
	"firempq/common"
	"firempq/svcerr"
	"firempq/util"
	"github.com/jmhodges/levigo"
	"github.com/op/go-logging"
	"sync"
	"time"
)

var log = logging.MustGetLogger("ldb")

// Item iterator built on top of LevelDB.
// It takes into account service name to limit the amount of selected data.
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
func makePayloadId(svcName, id string) string {
	return "p:" + svcName + ":" + id
}

// Item Id.
func makeItemId(svcName, id string) string {
	return "m:" + svcName + ":" + id
}

// Item will be stored into cache including payload.
func (ds *DataStorage) StoreItemWithPayload(svcName string, item common.IItemMetaData, payload string) {
	itemId := makeItemId(svcName, item.GetId())
	payloadId := makePayloadId(svcName, item.GetId())

	itemBody := item.ToBinary()

	ds.cacheLock.Lock()
	ds.itemCache[itemId] = itemBody
	ds.payloadCache[payloadId] = payload
	ds.cacheLock.Unlock()
}

func (ds *DataStorage) StoreItem(svcName string, item common.IItemMetaData) {
	itemId := makeItemId(svcName, item.GetId())

	itemBody := item.ToBinary()

	ds.cacheLock.Lock()
	ds.itemCache[itemId] = itemBody
	ds.cacheLock.Unlock()
}

// Updates item metadata, affects cache only until flushed.
func (ds *DataStorage) UpdateItem(svcName string, item common.IItemMetaData) {
	itemId := makeItemId(svcName, item.GetId())
	itemBody := item.ToBinary()
	ds.cacheLock.Lock()
	ds.itemCache[itemId] = itemBody
	ds.cacheLock.Unlock()
}

// Deletes item metadata and payload, affects cache only until flushed.
func (ds *DataStorage) DeleteItem(svcName string, itemId string) {
	id := makeItemId(svcName, itemId)
	payloadId := makePayloadId(svcName, itemId)
	ds.cacheLock.Lock()
	ds.itemCache[id] = nil
	ds.payloadCache[payloadId] = ""
	ds.cacheLock.Unlock()
}

// Returns item payload. Three places are checked:
// 1. Top level cache.
// 2. Temp cache while data is getting flushed into db.
// 3. If not found in cache, will do actually DB lookup.
func (ds *DataStorage) GetPayload(svcName string, itemId string) string {
	payloadId := makePayloadId(svcName, itemId)
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
// Service name used as a prefix to iterate over all item metadata that belongs to the specific service.
func (ds *DataStorage) IterService(svcName string) *ItemIterator {
	ropts := levigo.NewReadOptions()
	iter := ds.db.NewIterator(ropts)
	prefix := []byte(makeItemId(svcName, ""))
	return NewIter(iter, prefix)
}

const SVC_META_PREFIX = "qmeta:"
const SVC_SETTINGS_PREFIX = "qsettings:"

// Read all available services.
func (ds *DataStorage) GetAllServiceMeta() []*common.ServiceMetaInfo {
	qmiList := make([]*common.ServiceMetaInfo, 0, 1000)
	qtPrefix := []byte(SVC_META_PREFIX)
	ropts := levigo.NewReadOptions()
	iter := ds.db.NewIterator(ropts)
	iter.Seek(qtPrefix)
	for iter.Valid() {
		svcKey := iter.Key()
		svcName := bytes.TrimPrefix(svcKey, qtPrefix)

		if len(svcKey) == len(svcName) {
			break
		}
		smi, err := common.ServiceInfoFromBinary(iter.Value())
		if err != nil {
			log.Error("Coudn't read service meta data because of: %s", err.Error())
			iter.Next()
			continue
		}
		if smi.Disabled {
			log.Error("Service is disabled. Skipping: %s", smi.Name)
			iter.Next()
			continue
		}
		qmiList = append(qmiList, smi)
		iter.Next()
	}
	return qmiList
}

func (ds *DataStorage) SaveServiceMeta(qmi *common.ServiceMetaInfo) {
	key := SVC_META_PREFIX + qmi.Name
	wopts := levigo.NewWriteOptions()
	ds.db.Put(wopts, []byte(key), qmi.ToBinary())
}

func makeSettingsKey(svcName string) []byte {
	return []byte(SVC_SETTINGS_PREFIX + svcName)
}

// Read service settings bases on service name. Caller should profide correct settings structure to read binary data.
func (ds *DataStorage) GetServiceSettings(settings interface{}, svcName string) error {
	key := makeSettingsKey(svcName)
	ropts := levigo.NewReadOptions()
	data, _ := ds.db.Get(ropts, key)
	if data == nil {
		return svcerr.InvalidRequest("No service settings found: " + svcName)
	}
	err := util.StructFromBinary(settings, data)
	return err
}

func (ds *DataStorage) SaveServiceSettings(svcName string, settings interface{}) {
	key := makeSettingsKey(svcName)
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
		//ds.db.CompactRange(levigo.Range{nil, nil})
		ds.db.Close()
	} else {
		log.Error("Attempt to close database more than once!")
	}
}
