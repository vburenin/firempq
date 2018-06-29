package apis

// ItemIterator is an iterator interface over key/value storage.
type ItemIterator interface {
	Next() error
	GetData() []byte
	Valid() bool
	Close() error
}

type PayloadLocation struct {
	FileID   int64
	Position int64
}

type DataStorage interface {
	Flush() error
	GetStats() map[string]interface{}
	Close() error

	AddPayload(payload []byte) (int64, int64, error)
	RetrievePayload(fileID, pos int64) ([]byte, error)

	AddMetadata(metadata []byte) error
}
