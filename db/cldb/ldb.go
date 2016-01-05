package cldb

// Level DB cached wrapper to improve write performance using batching.

import (
	"firempq/common"
	"firempq/conf"
	"firempq/log"
	"sync"
	"time"

	. "firempq/api"

	"github.com/jmhodges/levigo"
)

// Default LevelDB read options.
var defaultReadOptions = levigo.NewReadOptions()

// Default LevelDB write options.
var defaultWriteOptions = levigo.NewWriteOptions()

// CLevelDBStorage A high level cached structure on top of LevelDB.
// It caches item storing them into database
// as multiple large batches later.
type CLevelDBStorage struct {
	db             *levigo.DB        // Pointer the the instance of level db.
	dbName         string            // LevelDB database name.
	itemCache      map[string]string // Active cache for item metadata.
	tmpItemCache   map[string]string // Active cache during flush operation.
	cacheLock      sync.Mutex        // Used for caches access.
	flushLock      sync.Mutex        // Used to prevent double flush.
	saveLock       sync.Mutex        // Used to prevent double flush.
	closed         bool
	flushSync      *sync.WaitGroup // Use to wait until flush happens.
	forceFlushChan chan bool
}

// NewLevelDBStorage is a constructor of DataStorage.
func NewLevelDBStorage(dbName string) (*CLevelDBStorage, error) {
	ds := CLevelDBStorage{
		dbName:         dbName,
		itemCache:      make(map[string]string),
		tmpItemCache:   nil,
		closed:         false,
		forceFlushChan: make(chan bool, 1),
		flushSync:      &sync.WaitGroup{},
	}

	// LevelDB write options.
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

func (ds *CLevelDBStorage) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"DbName":       ds.dbName,
		"CacheSize":    len(ds.itemCache),
		"TmpCacheSize": len(ds.tmpItemCache),
		"Closed":       ds.closed,
	}
}

func (ds *CLevelDBStorage) periodicCacheFlush() {
	ds.flushSync.Add(1)
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
func (ds *CLevelDBStorage) WaitFlush() {
	ds.flushLock.Lock()
	s := ds.flushSync
	ds.flushLock.Unlock()
	s.Wait()
}

// CachedStoreItem stores data into the cache.
func (ds *CLevelDBStorage) CachedStore(data ...string) {
	if len(data)%2 != 0 {
		panic("Number of arguments must be even!")
	}
	ds.cacheLock.Lock()
	for i := 0; i < len(data); i += 2 {
		ds.itemCache[data[i]] = data[i+1]
	}
	ds.cacheLock.Unlock()
}

// DeleteDataWithPrefix deletes all service data such as service metadata, items and payloads.
func (ds *CLevelDBStorage) DeleteDataWithPrefix(prefix string) int {
	ds.flushLock.Lock()
	ds.FlushCache()

	limitCounter := 0
	total := 0
	iter := ds.IterData(prefix)
	wb := levigo.NewWriteBatch()

	for iter.Valid() {
		if limitCounter < 10000 {
			key := iter.GetKey()
			iter.Next()
			wb.Delete(key)
			limitCounter++
			total++
		} else {
			limitCounter = 0
			ds.db.Write(defaultWriteOptions, wb)
			wb.Clear()
			wb = levigo.NewWriteBatch()
		}

	}
	if limitCounter > 0 {
		ds.db.Write(defaultWriteOptions, wb)
	}
	wb.Close()
	ds.flushLock.Unlock()
	return total
}

// FlushCache flushes all cache into database.
func (ds *CLevelDBStorage) FlushCache() {
	ds.saveLock.Lock()
	defer ds.saveLock.Unlock()
	ds.cacheLock.Lock()
	ds.tmpItemCache = ds.itemCache
	ds.itemCache = make(map[string]string)
	ds.cacheLock.Unlock()

	if len(ds.tmpItemCache) == 0 {
		return
	}
	wb := levigo.NewWriteBatch()
	counter := 0
	for k, v := range ds.tmpItemCache {
		if counter >= 10000 {
			ds.db.Write(defaultWriteOptions, wb)
			wb.Clear()
			counter = 0
		}
		key := common.UnsafeStringToBytes(k)
		if v == "" {
			wb.Delete(key)
		} else {
			wb.Put(key, common.UnsafeStringToBytes(v))
		}
		counter++
	}
	if counter > 0 {
		ds.db.Write(defaultWriteOptions, wb)
	}
	wb.Close()
}

// IterData returns an iterator over all data with prefix.
func (ds *CLevelDBStorage) IterData(prefix string) ItemIterator {
	iter := ds.db.NewIterator(defaultReadOptions)
	return makeItemIterator(iter, []byte(prefix))
}

// StoreData data directly into the database stores service metadata into database.
func (ds *CLevelDBStorage) StoreData(data ...string) error {
	if len(data)%2 != 0 {
		panic("Number of arguments must be even!")
	}
	wb := levigo.NewWriteBatch()
	for i := 0; i < len(data); i += 2 {
		wb.Put(common.UnsafeStringToBytes(data[i]),
			common.UnsafeStringToBytes(data[i+1]))
	}
	res := ds.db.Write(defaultWriteOptions, wb)
	wb.Close()
	return res
}

func (ds *CLevelDBStorage) DeleteData(id ...string) {
	wb := levigo.NewWriteBatch()
	for _, i := range id {
		wb.Delete(common.UnsafeStringToBytes(i))
	}
	ds.db.Write(defaultWriteOptions, wb)
	wb.Close()
}

// GetData looks data looks for and item going through each layer of cache finally looking into database.
func (ds *CLevelDBStorage) GetData(id string) string {
	ds.cacheLock.Lock()
	data, ok := ds.itemCache[id]
	if ok {
		ds.cacheLock.Unlock()
		return data
	}
	data, ok = ds.tmpItemCache[id]
	if ok {
		ds.cacheLock.Unlock()
		return data
	}
	ds.cacheLock.Unlock()
	value, _ := ds.db.Get(defaultReadOptions, common.UnsafeStringToBytes(id))
	return common.UnsafeBytesToString(value)
}

// CachedDeleteData deletes item metadata and payload, affects cache only until flushed.
func (ds *CLevelDBStorage) CachedDeleteData(id ...string) {
	ds.cacheLock.Lock()
	for _, i := range id {
		ds.itemCache[i] = ""
	}
	ds.cacheLock.Unlock()
}

func (ds *CLevelDBStorage) IsClosed() bool {
	ds.flushLock.Lock()
	cl := ds.closed
	ds.flushLock.Unlock()
	return cl
}

// Close flushes data on disk and closes database.
func (ds *CLevelDBStorage) Close() {
	ds.flushLock.Lock()
	if !ds.closed {
		log.Info("Flushing database cache")
		ds.FlushCache()
		ds.closed = true
		log.Info("Closing the database")
		ds.db.Close()
		log.Info("The database has been closed.")
	} else {
		log.Error("Attempt to close database more than once!")
	}
	ds.flushLock.Unlock()
}
