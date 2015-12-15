package db

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

// LevelDBStorage A high level cached structure on top of LevelDB.
// It caches item storing them into database
// as multiple large batches later.
type LevelDBStorage struct {
	db             *levigo.DB        // Pointer the the instance of level db.
	dbName         string            // LevelDB database name.
	itemCache      map[string]string // Active cache for item metadata.
	tmpItemCache   map[string]string // Active cache during flush operation.
	cacheLock      sync.Mutex        // Used for caches access.
	flushLock      sync.Mutex        // Used to prevent double flush.
	closed         bool
	flushSync      *sync.WaitGroup // Use to wait until flush happens.
	forceFlushChan chan bool
}

// NewLevelDBStorage is a constructor of DataStorage.
func NewLevelDBStorage(dbName string) (*LevelDBStorage, error) {
	ds := LevelDBStorage{
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

func (ds *LevelDBStorage) periodicCacheFlush() {
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
			ds.flushCache()
		}
		oldFlushSync.Done()
		ds.flushLock.Unlock()
	}
	ds.flushSync.Done()
}

// WaitFlush waits until all data is flushed on disk.
func (ds *LevelDBStorage) WaitFlush() {
	ds.flushLock.Lock()
	s := ds.flushSync
	ds.flushLock.Unlock()
	s.Wait()
}

// CachedStoreItem stores data into the cache.
func (ds *LevelDBStorage) CachedStore(data ...string) {
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
func (ds *LevelDBStorage) DeleteDataWithPrefix(prefix string) int {
	ds.flushLock.Lock()
	defer ds.flushLock.Unlock()
	ds.flushCache()

	limitCounter := 0
	total := 0
	iter := ds.IterData(prefix)
	wb := levigo.NewWriteBatch()

	for iter.Valid() {
		total++
		if limitCounter < 1000 {
			wb.Delete(iter.GetKey())
			limitCounter++
		} else {
			limitCounter = 0
			ds.db.Write(defaultWriteOptions, wb)
			wb = levigo.NewWriteBatch()
		}
		iter.Next()
	}

	ds.db.Write(defaultWriteOptions, wb)
	return total
}

// FlushCache flushes all cache into database.
func (ds *LevelDBStorage) flushCache() {
	ds.cacheLock.Lock()
	ds.tmpItemCache = ds.itemCache
	ds.itemCache = make(map[string]string)
	ds.cacheLock.Unlock()

	wb := levigo.NewWriteBatch()
	for k, v := range ds.tmpItemCache {
		key := common.UnsafeStringToBytes(k)
		if v == "" {
			wb.Delete(key)
		} else {
			wb.Put(key, common.UnsafeStringToBytes(v))
		}
	}
	ds.db.Write(defaultWriteOptions, wb)
}

// IterData returns an iterator over all data with prefix.
func (ds *LevelDBStorage) IterData(prefix string) ItemIterator {
	iter := ds.db.NewIterator(defaultReadOptions)
	return makeItemIterator(iter, common.UnsafeStringToBytes(prefix))
}

// StoreData data directly into the database stores service metadata into database.
func (ds *LevelDBStorage) StoreData(data ...string) error {
	if len(data)%2 != 0 {
		panic("Number of arguments must be even!")
	}
	wb := levigo.NewWriteBatch()
	for i := 0; i < len(data); i += 2 {
		wb.Put(common.UnsafeStringToBytes(data[i]),
			common.UnsafeStringToBytes(data[i+1]))
	}
	return ds.db.Write(defaultWriteOptions, wb)
}

func (ds *LevelDBStorage) DeleteData(id ...string) {
	wb := levigo.NewWriteBatch()
	for _, i := range id {
		wb.Delete(common.UnsafeStringToBytes(i))
	}
	ds.db.Write(defaultWriteOptions, wb)
}

// GetData looks data looks for and item going through each layer of cache finally looking into database.
func (ds *LevelDBStorage) GetData(id string) string {
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
func (ds *LevelDBStorage) CachedDeleteData(id ...string) {
	ds.cacheLock.Lock()
	for _, i := range id {
		ds.itemCache[i] = ""
	}
	ds.cacheLock.Unlock()
}

func (ds *LevelDBStorage) IsClosed() bool {
	ds.flushLock.Lock()
	defer ds.flushLock.Unlock()
	return ds.closed
}

// Close flushes data on disk and closes database.
func (ds *LevelDBStorage) Close() {
	ds.flushLock.Lock()
	defer ds.flushLock.Unlock()
	if !ds.closed {
		ds.flushCache()
		ds.closed = true
		ds.db.Close()
	} else {
		log.Error("Attempt to close database more than once!")
	}
}
