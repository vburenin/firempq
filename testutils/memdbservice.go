package testutils

import (
	"sync"

	. "firempq/api"
	"sort"
	"strings"
)

type InMemDBService struct {
	mutex   sync.Mutex
	mapData map[string]string
	closed  bool
}

func NewInMemDBService() *InMemDBService {
	return &InMemDBService{
		mapData: make(map[string]string),
	}
}

func (d *InMemDBService) WaitFlush()     {}
func (d *InMemDBService) Close()         { d.closed = true }
func (d *InMemDBService) IsClosed() bool { return d.closed }

func (d *InMemDBService) CachedStore(data ...string) {
	if len(data)%2 != 0 {
		panic("Number of arguments should be even!")
	}
	d.mutex.Lock()
	for i := 0; i < len(data); i += 2 {
		d.mapData[data[i]] = data[i+1]
	}
	d.mutex.Unlock()
}

func (d *InMemDBService) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"Size":   len(d.mapData),
		"Closed": d.closed,
	}
}

func (d *InMemDBService) DeleteDataWithPrefix(prefix string) int {
	d.mutex.Lock()
	keys := make([]string, 10)
	for k, _ := range d.mapData {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		delete(d.mapData, k)
	}
	d.mutex.Unlock()
	return len(keys)
}

func (d *InMemDBService) StoreData(data ...string) error {
	d.CachedStore(data...)
	return nil
}

func (d *InMemDBService) DeleteData(id ...string) {
	d.mutex.Lock()
	for _, k := range id {
		delete(d.mapData, k)
	}
	d.mutex.Unlock()
}

func (d *InMemDBService) CachedDeleteData(id ...string) {
	d.DeleteData(id...)
}

func (d *InMemDBService) IterData(prefix string) ItemIterator {
	return NewInMemIterator(d.mapData, prefix)
}

func (d *InMemDBService) GetData(id string) string {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.mapData[id]
}

func (d *InMemDBService) FlushCache() {}

type InMemIterator struct {
	keyList []string
	curPos  int
	prefix  string
	data    map[string]string
}

func NewInMemIterator(data map[string]string, prefix string) *InMemIterator {
	keylist := make([]string, 0, 100)
	dataCopy := make(map[string]string)
	for k, v := range data {
		if strings.HasPrefix(k, prefix) {
			keylist = append(keylist, k)
			dataCopy[k] = v
		}
	}
	sort.Strings(keylist)

	return &InMemIterator{
		keyList: keylist,
		prefix:  prefix,
		curPos:  0,
		data:    dataCopy,
	}
}

func (iter *InMemIterator) Next() {
	iter.curPos++
}

func (iter *InMemIterator) Valid() bool {
	return iter.curPos < len(iter.keyList)
}

func (iter *InMemIterator) GetValue() []byte {
	return []byte(iter.data[iter.keyList[iter.curPos]])
}

func (iter *InMemIterator) GetKey() []byte {
	return []byte(iter.keyList[iter.curPos])
}

func (iter *InMemIterator) GetTrimKey() []byte {
	return iter.GetKey()[len(iter.prefix):]
}

func (iter *InMemIterator) Close() {}
