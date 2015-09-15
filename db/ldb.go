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
	"sync"
	"time"

	"github.com/jmhodges/levigo"
	"github.com/op/go-logging"
)

// Default LevelDB read options.
var defaultReadOptions = levigo.NewReadOptions()

// Default LevelDB write options.
var defaultWriteOptions = levigo.NewWriteOptions()

var log = logging.MustGetLogger("ldb")

// ItemIterator built on top of LevelDB.
// It takes into account service name to limit the amount of selected data.
type ItemIterator struct {
	iter   *levigo.Iterator
	prefix []byte // Prefix for look ups.
	Key    []byte // Currently selected key. Valid only if the iterator is valid.
	Value  []byte // Currently selected value. Valid only if the iterator is valid.
}

//
func makeItemIterator(iter *levigo.Iterator, prefix []byte) *ItemIterator {
	iter.Seek(prefix)
	return &ItemIterator{iter, prefix, nil, nil}
}

// Next switches to the next element.
func (mi *ItemIterator) Next() {
	mi.iter.Next()
}

// Valid returns true if the current value is OK, otherwise false.
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

// Close closes iterator. Iterator must be closed!
func (mi *ItemIterator) Close() {
	mi.iter.Close()
}

// DataStorage A high level structure on top of LevelDB.
// It caches item storing them into database
// as multiple large batches later.
type DataStorage struct {
	db              *levigo.DB        // Pointer the the instance of level db.
	dbName          string            // LevelDB database name.
	itemCache       map[string][]byte // Active cache for item metadata.
	payloadCache    map[string]string // Active cache for item payload.
	tmpPayloadCache map[string]string // Temporary map of cached payloads while it is in process of flushing into DB.
	cacheLock       sync.Mutex        // Used for caches access.
	flushLock       sync.Mutex        // Used to prevent double flush.
	closed          bool
}

// NewDataStorage is a constructor of DataStorage.
func NewDataStorage(dbName string) (*DataStorage, error) {
	ds := DataStorage{
		dbName:          dbName,
		itemCache:       make(map[string][]byte),
		payloadCache:    make(map[string]string),
		tmpPayloadCache: make(map[string]string),
		closed:          false,
	}
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(10 * 1024 * 1024)
	opts.SetCompression(levigo.SnappyCompression)
	db, err := levigo.Open(dbName, opts)
	if err != nil {
		return nil, err
	}
	ds.db = db
	go ds.periodicCacheFlush()
	return &ds, nil
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
func makePayloadID(svcName, id string) string {
	return svcName + "\x01" + id
}

// Item Id.
func makeItemID(svcName, id string) string {
	return svcName + "\x02" + id
}

// StoreItemWithPayload stores item and provide payload.
func (ds *DataStorage) StoreItemWithPayload(svcName string, item common.IItemMetaData, payload string) {
	itemBody, err := item.Marshal()
	if err != nil {
		log.Error(err.Error())
		return
	}

	itemID := makeItemID(svcName, item.GetId())
	payloadID := makePayloadID(svcName, item.GetId())
	ds.cacheLock.Lock()
	ds.itemCache[itemID] = itemBody
	ds.payloadCache[payloadID] = payload
	ds.cacheLock.Unlock()
}

// StoreItem stores item onlt without paylaod.
func (ds *DataStorage) StoreItem(svcName string, item common.IItemMetaData) {
	itemBody, err := item.Marshal()
	if err != nil {
		log.Error(err.Error())
		return
	}
	itemID := makeItemID(svcName, item.GetId())

	ds.cacheLock.Lock()
	ds.itemCache[itemID] = itemBody
	ds.cacheLock.Unlock()
}

// DeleteItem deletes item metadata and payload, affects cache only until flushed.
func (ds *DataStorage) DeleteItem(svcName string, itemID string) {
	id := makeItemID(svcName, itemID)
	payloadID := makePayloadID(svcName, itemID)
	ds.cacheLock.Lock()
	ds.itemCache[id] = nil
	ds.payloadCache[payloadID] = ""
	ds.cacheLock.Unlock()
}

// DeleteServiceData deletes all service data such as service metadata, items and payloads.
func (ds *DataStorage) DeleteServiceData(svcName string) {

	counter := 0
	iter := ds.IterServiceItems(svcName)
	wb := levigo.NewWriteBatch()

	for iter.Valid() {
		if counter < 1000 {
			wb.Delete(iter.Key)
			counter++
		} else {
			counter = 0
			ds.db.Write(defaultWriteOptions, wb)
		}
	}

	wb.Delete([]byte(serviceMetadataPrefix + svcName))
	wb.Delete([]byte(serviceConfigPrefix + svcName))

	ds.db.Write(defaultWriteOptions, wb)
}

// GetPayload returns item payload. Three places are checked:
// 1. Top level cache.
// 2. Temp cache while data is getting flushed into db.
// 3. If not found in cache, will mane a DB lookup.
func (ds *DataStorage) GetPayload(svcName string, itemID string) string {
	payloadID := makePayloadID(svcName, itemID)
	ds.cacheLock.Lock()
	payload, ok := ds.payloadCache[payloadID]
	if ok {
		ds.cacheLock.Unlock()
		return payload
	}
	payload, ok = ds.tmpPayloadCache[payloadID]
	if ok {
		ds.cacheLock.Unlock()
		return payload
	}
	ds.cacheLock.Unlock()
	value, _ := ds.db.Get(defaultReadOptions, []byte(payloadID))
	return string(value)
}

// FlushCache flushes all cache into database.
func (ds *DataStorage) FlushCache() {
	if ds.closed {
		return
	}
	ds.cacheLock.Lock()
	tmpItemCache := ds.itemCache
	ds.tmpPayloadCache = ds.payloadCache
	ds.itemCache = make(map[string][]byte)
	ds.payloadCache = make(map[string]string)
	ds.cacheLock.Unlock()

	wb := levigo.NewWriteBatch()
	for k, v := range tmpItemCache {
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

	ds.db.Write(defaultWriteOptions, wb)

	ds.cacheLock.Lock()
	ds.tmpPayloadCache = make(map[string]string)
	ds.cacheLock.Unlock()
}

// IterServiceItems returns new over all service item metadata.
// Service name used as a prefix to file all service items.
func (ds *DataStorage) IterServiceItems(svcName string) *ItemIterator {
	iter := ds.db.NewIterator(defaultReadOptions)
	prefix := []byte(makeItemID(svcName, ""))
	return makeItemIterator(iter, prefix)
}

const serviceMetadataPrefix = ":meta:"
const serviceConfigPrefix = ":conf:"

// GetAllServiceMeta returns all available services.
func (ds *DataStorage) GetAllServiceMeta() []*common.ServiceMetaInfo {
	qmiList := make([]*common.ServiceMetaInfo, 0, 1000)
	qtPrefix := []byte(serviceMetadataPrefix)
	iter := ds.db.NewIterator(defaultReadOptions)
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

// SaveServiceMeta stores service metadata into database.
func (ds *DataStorage) SaveServiceMeta(qmi *common.ServiceMetaInfo) {
	key := serviceMetadataPrefix + qmi.Name
	ds.db.Put(defaultWriteOptions, []byte(key), qmi.ToBinary())
}

func makeSettingsKey(svcName string) []byte {
	return []byte(serviceConfigPrefix + svcName)
}

// GetServiceConfig reads service config bases on service name.
// Caller should provide correct settings structure to read binary data.
func (ds *DataStorage) GetServiceConfig(settings interface{}, svcName string) error {
	key := makeSettingsKey(svcName)
	data, _ := ds.db.Get(defaultReadOptions, key)
	if data == nil {
		return common.InvalidRequest("No service settings found: " + svcName)
	}
	err := common.StructFromBinary(settings, data)
	return err
}

// SaveServiceConfig saves service config into database.
func (ds *DataStorage) SaveServiceConfig(svcName string, settings interface{}) {
	key := makeSettingsKey(svcName)
	data := common.StructToBinary(settings)
	err := ds.db.Put(defaultWriteOptions, key, data)
	if err != nil {
		log.Error("Failed to save config: %s", err.Error())
	}
}

// Close flushes data on disk and closes database.
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
