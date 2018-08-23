package linear

type PayloadKey struct {
	FileID int64
	Offset int64
}

// PayloadCache is a data structure that keeps last X binary arrays in memory.
// New elements that are being added to the cache will eventually push out the oldest.
type PayloadCache struct {
	cache     map[PayloadKey][]byte
	keys      []PayloadKey
	sizeLimit int
	counter   int
}

func NewPayloadCache(sizeLimit int) *PayloadCache {
	return &PayloadCache{
		cache:     make(map[PayloadKey][]byte, sizeLimit+1),
		sizeLimit: sizeLimit,
		keys:      make([]PayloadKey, 0, sizeLimit*10),
	}
}

// AddPayload adds payload into the cache. Returns true if added element made is reached limit of the payload cache.
// Basically, it returns TRUE every time it reaches (counter % sizeLimit) == 0 element.
func (pc *PayloadCache) AddPayload(fileID, pos int64, data []byte) bool {
	pk := PayloadKey{FileID: fileID, Offset: pos}
	pc.cache[pk] = data
	pc.keys = append(pc.keys, pk)
	if len(pc.keys) > pc.sizeLimit {
		delete(pc.cache, pc.keys[0])
		pc.keys = pc.keys[1:]
		// To reduce memory allocations, recreate array to have more room to store keys.
		if len(pc.keys) == cap(pc.keys) {
			newKeys := make([]PayloadKey, len(pc.keys), pc.sizeLimit*10)
			copy(newKeys, pc.keys)
			pc.keys = newKeys
		}
	}
	pc.counter++
	if pc.counter == pc.sizeLimit {
		pc.counter = 0
	}
	return pc.counter == 0
}

func (pc *PayloadCache) Payload(fileID, pos int64) []byte {
	return pc.cache[PayloadKey{FileID: fileID, Offset: pos}]
}
