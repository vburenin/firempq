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
	"firempq/conf"
	"firempq/iface"
	"sort"
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
	flushSync       *sync.WaitGroup // Use to wait until flush happens.
	forceFlushChan  chan bool
}

// NewDataStorage is a constructor of DataStorage.
func NewDataStorage(dbName string) (*DataStorage, error) {
	ds := DataStorage{
		dbName:          dbName,
		itemCache:       make(map[string][]byte),
		payloadCache:    make(map[string]string),
		tmpPayloadCache: make(map[string]string),
		closed:          false,
		forceFlushChan:  make(chan bool, 1),
		flushSync:       &sync.WaitGroup{},
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
	ds.flushSync.Add(1)
	go ds.periodicCacheFlush()
	return &ds, nil
}

func (ds *DataStorage) periodicCacheFlush() {
	for !ds.closed {
		select {
		case <-ds.forceFlushChan:
			break
		case <-time.After(conf.CFG.DbFlushInterval * time.Millisecond):
			break
		}
		ds.flushLock.Lock()
		oldFlushSync := ds.flushSync
		ds.flushSync = &sync.WaitGroup{}
		ds.flushSync.Add(1)
		if !ds.closed {
			ds.FlushCache()
		}
		oldFlushSync.Done()
		ds.flushLock.Unlock()
	}
	ds.flushSync.Done()
}

// WaitFlush waits until all data is flushed on disk.
func (ds *DataStorage) WaitFlush() {
	ds.flushLock.Lock()
	s := ds.flushSync
	ds.flushLock.Unlock()
	s.Wait()
}

// Item payload Id.
func makePayloadID(serviceId, id string) string {
	return serviceId + "\x01" + id
}

// Item Id.
func makeItemID(serviceId, id string) string {
	return serviceId + "\x02" + id
}

// StoreItemWithPayload stores item and provide payload.
func (ds *DataStorage) StoreItemWithPayload(serviceId string, item iface.IItemMetaData, payload string) {
	itemBody, err := item.Marshal()
	if err != nil {
		log.Error(err.Error())
		return
	}

	itemID := makeItemID(serviceId, item.GetId())
	payloadID := makePayloadID(serviceId, item.GetId())
	ds.cacheLock.Lock()
	ds.itemCache[itemID] = itemBody
	ds.payloadCache[payloadID] = payload
	ds.cacheLock.Unlock()
}

// StoreItem stores item onlt without paylaod.
func (ds *DataStorage) StoreItem(serviceId string, item iface.IItemMetaData) {
	itemBody, err := item.Marshal()
	if err != nil {
		log.Error(err.Error())
		return
	}
	itemID := makeItemID(serviceId, item.GetId())

	ds.cacheLock.Lock()
	ds.itemCache[itemID] = itemBody
	ds.cacheLock.Unlock()
}

// DeleteItem deletes item metadata and payload, affects cache only until flushed.
func (ds *DataStorage) DeleteItem(serviceId string, itemID string) {
	id := makeItemID(serviceId, itemID)
	payloadID := makePayloadID(serviceId, itemID)
	ds.cacheLock.Lock()
	ds.itemCache[id] = nil
	ds.payloadCache[payloadID] = ""
	ds.cacheLock.Unlock()
}

// DeleteServiceData deletes all service data such as service metadata, items and payloads.
func (ds *DataStorage) DeleteServiceData(serviceId string) {

	counter := 0
	iter := ds.IterServiceItems(serviceId)
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

	wb.Delete([]byte(serviceDescriptionPrefix + serviceId))
	wb.Delete([]byte(serviceConfigPrefix + serviceId))

	ds.db.Write(defaultWriteOptions, wb)
}

// GetPayload returns item payload. Three places are checked:
// 1. Top level cache.
// 2. Temp cache while data is getting flushed into db.
// 3. If not found in cache, will mane a DB lookup.
func (ds *DataStorage) GetPayload(serviceId string, itemID string) string {
	payloadID := makePayloadID(serviceId, itemID)
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
		key := common.UnsafeStringToBytes(k)
		if v == nil {
			wb.Delete(key)
		} else {
			wb.Put(key, v)
		}
	}

	for k, v := range ds.tmpPayloadCache {
		key := common.UnsafeStringToBytes(k)
		if v == "" {
			wb.Delete(key)
		} else {
			wb.Put(key, common.UnsafeStringToBytes(v))
		}
	}

	ds.db.Write(defaultWriteOptions, wb)

	ds.cacheLock.Lock()
	ds.tmpPayloadCache = make(map[string]string)
	ds.cacheLock.Unlock()
}

// IterServiceItems returns new over all service item metadata.
// Service name used as a prefix to file all service items.
func (ds *DataStorage) IterServiceItems(serviceId string) *ItemIterator {
	iter := ds.db.NewIterator(defaultReadOptions)
	prefix := common.UnsafeStringToBytes(makeItemID(serviceId, ""))
	return makeItemIterator(iter, prefix)
}

const serviceDescriptionPrefix = ":desc:"
const serviceConfigPrefix = ":conf:"

// GetAllServiceMeta returns all available services.
func (ds *DataStorage) GetServiceDescriptions() []*common.ServiceDescription {
	sdList := make(common.ServiceDescriptionList, 0, 1000)
	sdPrefix := []byte(serviceDescriptionPrefix)
	iter := ds.db.NewIterator(defaultReadOptions)
	iter.Seek(sdPrefix)
	for iter.Valid() {
		svcKey := iter.Key()
		svcName := bytes.TrimPrefix(svcKey, sdPrefix)

		if len(svcKey) == len(svcName) {
			break
		}
		serviceDesc, err := common.NewServiceDescriptionFromBinary(iter.Value())
		if err != nil {
			log.Error(
				"Coudn't read service '%s' description: %s",
				svcName, err.Error())
			iter.Next()
			continue
		}
		sdList = append(sdList, serviceDesc)
		iter.Next()
	}
	sort.Sort(sdList)
	return sdList
}

// SaveServiceMeta stores service metadata into database.
func (ds *DataStorage) SaveServiceMeta(serviceDescription *common.ServiceDescription) {
	key := serviceDescriptionPrefix + serviceDescription.GetName()
	data, _ := serviceDescription.Marshal()
	ds.db.Put(defaultWriteOptions, common.UnsafeStringToBytes(key), data)
}

func makeSettingsKey(serviceId string) []byte {
	return common.UnsafeStringToBytes(serviceConfigPrefix + serviceId)
}

// GetServiceConfig reads service config bases on service name.
// Caller should provide correct settings structure to read binary data.
func (ds *DataStorage) LoadServiceConfig(conf iface.Marshalable, serviceId string) error {
	key := makeSettingsKey(serviceId)
	data, _ := ds.db.Get(defaultReadOptions, key)
	if data == nil {
		return common.InvalidRequest("No service settings found: " + serviceId)
	}

	if err := conf.Unmarshal(data); err != nil {
		log.Error("Error in '%s' service settings: %s", serviceId, err.Error())
		return common.InvalidRequest("Service settings error: " + serviceId)
	}
	return nil
}

// SaveServiceConfig saves service config into database.
func (ds *DataStorage) SaveServiceConfig(serviceId string, conf iface.MarshalToBin) {
	key := makeSettingsKey(serviceId)
	data, _ := conf.Marshal()
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
		ds.db.Close()
	} else {
		log.Error("Attempt to close database more than once!")
	}
}
