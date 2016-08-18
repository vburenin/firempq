package apis

// ItemIterator is an iterator interface over key/value storage.
type ItemIterator interface {
	Next()
	Valid() bool
	Close()
	GetValue() []byte
	GetKey() []byte
	GetTrimKey() []byte
}

// DataStorage is an abstracted interface over the key/value storage
// currently the main purpose is to to prepend prefixes to the data key.
type DataStorage interface {
	WaitFlush()
	CachedStore2(key1 string, data1 []byte, key2 string, data2 []byte)
	CachedStore(key string, data []byte)
	DeleteDataWithPrefix(prefix string) int
	StoreData(key string, data []byte) error
	FlushCache()
	DeleteData(key ...string) error
	DeleteCacheData(id ...string)
	IterData(prefix string) ItemIterator
	GetData(id string) []byte
	GetStats() map[string]interface{}
	Close()
	IsClosed() bool
}
