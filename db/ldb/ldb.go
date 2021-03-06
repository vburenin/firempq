package ldb

// Level DB cached wrapper to improve write performance using batching.

import (
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/log"
)

type ItemCache map[string][]byte

// LevelDBStorage A high level cached structure on top of LevelDB.
// It caches item storing them into database
// as multiple large batches later.
type LevelDBStorage struct {
	cfg            *conf.Config
	db             *leveldb.DB // Pointer the the instance of level db.
	dbName         string      // LevelDB database name.
	itemCache      ItemCache   // Active cache for item metadata.
	tmpItemCache   ItemCache   // Active cache during flush operation.
	cacheLock      sync.Mutex  // Used for caches access.
	flushLock      sync.Mutex  // Used to prevent double flush.
	saveLock       sync.Mutex
	closed         bool
	flushSync      *sync.WaitGroup // Use to wait until flush happens.
	forceFlushChan chan bool
}

// NewLevelDBStorage is a constructor of DataStorage.
func NewLevelDBStorage(cfg *conf.Config) (*LevelDBStorage, error) {
	ds := LevelDBStorage{
		cfg:            cfg,
		dbName:         cfg.DatabasePath,
		itemCache:      make(ItemCache),
		tmpItemCache:   make(ItemCache),
		closed:         false,
		forceFlushChan: make(chan bool, 1),
		flushSync:      &sync.WaitGroup{},
	}

	// LevelDB write options.
	opts := new(opt.Options)
	opts.Compression = opt.NoCompression
	opts.BlockCacheCapacity = 8 * 1024 * 1024
	opts.WriteBuffer = 8 * 1024 * 1024

	db, err := leveldb.OpenFile(cfg.DatabasePath, opts)
	if err != nil {
		return nil, err
	}
	ds.db = db
	go ds.periodicCacheFlush()
	return &ds, nil
}

func (ds *LevelDBStorage) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"DBName":       ds.dbName,
		"CacheSize":    len(ds.itemCache),
		"TmpCacheSize": len(ds.tmpItemCache),
		"Close":        ds.closed,
	}
}

func (ds *LevelDBStorage) periodicCacheFlush() {
	ds.flushSync.Add(1)
	for !ds.closed {
		select {
		case <-ds.forceFlushChan:
			break
		case <-time.After(time.Duration(conf.CFG.DbFlushInterval) * time.Millisecond):
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
func (ds *LevelDBStorage) WaitFlush() {
	ds.flushLock.Lock()
	s := ds.flushSync
	ds.flushLock.Unlock()
	s.Wait()
}

// CachedStoreItem stores data into the cache.
func (ds *LevelDBStorage) CachedStore(key string, data []byte) {
	ds.cacheLock.Lock()
	ds.itemCache[key] = data
	ds.cacheLock.Unlock()
}

// CachedStoreItem2 stores two items into the item cache.
func (ds *LevelDBStorage) CachedStore2(key1 string, data1 []byte, key2 string, data2 []byte) {
	ds.cacheLock.Lock()
	ds.itemCache[key1] = data1
	ds.itemCache[key2] = data2
	ds.cacheLock.Unlock()
}

// DeleteDataWithPrefix deletes all service data such as service metadata, items and payloads.
func (ds *LevelDBStorage) DeleteDataWithPrefix(prefix string) int {
	ds.FlushCache()
	ds.saveLock.Lock()
	defer ds.saveLock.Unlock()

	limitCounter := 0
	total := 0
	iter := ds.IterData(prefix)
	wb := new(leveldb.Batch)

	for iter.Valid() {
		total++
		if limitCounter < 1000 {
			wb.Delete(iter.GetKey())
			limitCounter++
		} else {
			limitCounter = 0
			ds.db.Write(wb, nil)
			wb.Reset()
		}
		iter.Next()
	}

	ds.db.Write(wb, nil)
	return total
}

// FlushCache flushes all cache into database.
func (ds *LevelDBStorage) FlushCache() {
	ds.saveLock.Lock()
	ds.cacheLock.Lock()
	ds.tmpItemCache = ds.itemCache
	ds.itemCache = make(ItemCache)
	ds.cacheLock.Unlock()

	wb := &leveldb.Batch{}
	count := 0
	for k, v := range ds.tmpItemCache {
		if count >= 100 {
			ds.db.Write(wb, nil)
			wb.Reset()
			count = 0
		}
		key := enc.UnsafeStringToBytes(k)
		if v == nil {
			wb.Delete(key)
		} else {
			wb.Put(key, v)
		}
		count++

	}
	ds.db.Write(wb, nil)
	ds.saveLock.Unlock()
}

// IterData returns an iterator over all data with prefix.
func (ds *LevelDBStorage) IterData(prefix string) apis.ItemIterator {
	iter := ds.db.NewIterator(new(util.Range), nil)
	return makeItemIterator(iter, enc.UnsafeStringToBytes(prefix))
}

// StoreData data directly into the database stores service metadata into database.
func (ds *LevelDBStorage) StoreData(key string, data []byte) error {
	return ds.db.Put(enc.UnsafeStringToBytes(key), data, nil)
}

func (ds *LevelDBStorage) DeleteData(id ...string) error {
	wb := new(leveldb.Batch)
	for _, i := range id {
		wb.Delete(enc.UnsafeStringToBytes(i))
	}
	return ds.db.Write(wb, nil)
}

// GetData looks data looks for and item going through each layer of cache finally looking into database.
func (ds *LevelDBStorage) GetData(id string) []byte {
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
	value, e := ds.db.Get(enc.UnsafeStringToBytes(id), nil)
	if e != nil {
		return nil
	}
	return value
}

// CachedDeleteData deletes item metadata and payload, affects cache only until flushed.
func (ds *LevelDBStorage) DeleteCacheData(id ...string) {
	ds.cacheLock.Lock()
	for _, i := range id {
		ds.itemCache[i] = nil
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
		log.Info("Flushing database cache")
		ds.FlushCache()
		ds.closed = true
		log.Info("Closing the database")
		ds.db.Close()
		log.Info("The database has been closed.")
	} else {
		log.Error("Attempt to close database more than once!")
	}
}
