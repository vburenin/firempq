package testutils

import (
	"sync"

	. "firempq/api"
	"sort"
	"strings"
)

type InMemDBService struct {
	mutex   sync.Mutex
	mapData map[string][]byte
	closed  bool
}

func NewInMemDBService() *InMemDBService {
	return &InMemDBService{
		mapData: make(map[string][]byte),
	}
}

func (d *InMemDBService) WaitFlush()     {}
func (d *InMemDBService) Close()         { d.closed = true }
func (d *InMemDBService) IsClosed() bool { return d.closed }

func (d *InMemDBService) FastStoreData(id string, data []byte) {
	d.mutex.Lock()
	d.mapData[id] = data
	d.mutex.Unlock()
}

func (d *InMemDBService) FastStoreData2(id1 string, data1 []byte, id2 string, data2 []byte) {
	d.mutex.Lock()
	d.mapData[id1] = data1
	d.mapData[id2] = data2
	d.mutex.Unlock()
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

func (d *InMemDBService) StoreData(id string, data []byte) error {
	d.FastStoreData(id, data)
	return nil
}

func (d *InMemDBService) DeleteData(id string) {
	d.mutex.Lock()
	delete(d.mapData, id)
	d.mutex.Unlock()
}

func (d *InMemDBService) FastDeleteData(id ...string) {
	d.mutex.Lock()
	for _, k := range id {
		delete(d.mapData, k)
	}
	d.mutex.Unlock()
}

func (d *InMemDBService) IterData(prefix string) ItemIterator {
	return NewInMemIterator(d.mapData, prefix)
}

func (d *InMemDBService) GetData(id string) []byte {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.mapData[id]
}

type InMemIterator struct {
	keyList []string
	curPos  int
	prefix  string
	data    map[string][]byte
}

func NewInMemIterator(data map[string][]byte, prefix string) *InMemIterator {
	keylist := make([]string, 0, 100)
	dataCopy := make(map[string][]byte)
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
	return iter.data[iter.keyList[iter.curPos]]
}

func (iter *InMemIterator) GetKey() []byte {
	return []byte(iter.keyList[iter.curPos])
}

func (iter *InMemIterator) GetTrimKey() []byte {
	return iter.GetKey()[len(iter.prefix):]
}

func (iter *InMemIterator) Close() {}
