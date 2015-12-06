package db

import (
	"bytes"

	"github.com/jmhodges/levigo"
)

// ItemIterator built on top of LevelDB.
// It takes into account service name to limit the amount of selected data.
type ItemIterator struct {
	iter    *levigo.Iterator
	prefix  []byte // Prefix for look ups.
	Key     []byte // Currently selected key. Valid only if the iterator is valid.
	Value   []byte // Currently selected value. Valid only if the iterator is valid.
	TrimKey []byte
}

//
func makeItemIterator(iter *levigo.Iterator, prefix []byte) *ItemIterator {
	iter.Seek(prefix)
	return &ItemIterator{iter, prefix, nil, nil, nil}
}

// Next switches to the next element.
func (mi *ItemIterator) Next() {
	mi.iter.Next()
}

// Valid returns true if the current value is OK, otherwise false.
// for iter.Valid() {
//    mykey := iter.Key
//    myvalue := iter.Value
//    ......
//    iter.Next()
//}
func (mi *ItemIterator) Valid() bool {
	valid := mi.iter.Valid()
	if valid {
		k := mi.iter.Key()
		// Strip key prefix. If prefix doesn't match the length of the slice will remain the same.

		if !bytes.HasPrefix(k, mi.prefix) {
			return false
		}
		mi.TrimKey = k[len(mi.prefix):]
		mi.Key = k
		mi.Value = mi.iter.Value()
		return true
	}
	return false
}

// Close closes iterator. Iterator must be closed!
func (mi *ItemIterator) Close() {
	mi.iter.Close()
}
