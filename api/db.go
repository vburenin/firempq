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
	FastStoreData(id string, data []byte)
	FastStoreData2(id1 string, data1 []byte, id2 string, data2 []byte)
	DeleteDataWithPrefix(prefix string) int
	StoreData(id string, data []byte) error
	DeleteData(id string)
	FastDeleteData(id ...string)
	IterData(prefix string) ItemIterator
	GetData(id string) []byte
	Close()
	IsClosed() bool
}
