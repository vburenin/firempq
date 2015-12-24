package api

type ItemIterator interface {
	Next()
	Valid() bool
	Close()
	GetValue() []byte
	GetKey() []byte
	GetTrimKey() []byte
}

type DataStorage interface {
	WaitFlush()
	CachedStore(data ...string)
	DeleteDataWithPrefix(prefix string) int
	StoreData(data ...string) error
	DeleteData(id ...string)
	CachedDeleteData(id ...string)
	IterData(prefix string) ItemIterator
	GetData(id string) string
	GetStats() map[string]interface{}
	Close()
	IsClosed() bool
}
