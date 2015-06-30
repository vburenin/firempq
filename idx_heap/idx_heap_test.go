package idx_heap

import (
	"strconv"
	"testing"
	"time"
)

func TestHeap1(t *testing.T) {
	st := time.Now().UnixNano()
	mh := NewIndexHeap()

    var i int
    for i = 0; i < 100; i++ {
        mh.PushItem(&HeapItem{strconv.Itoa(i), int64(i)})
    }

    e_time := time.Now().UnixNano()
    println((e_time - st) / 1000000)
    println("--------")

    st = time.Now().UnixNano()

    kk := 0
    for mh.Len() > 0 {
        mh.PopItem()
        kk += 1
    }
    e_time = time.Now().UnixNano()
    println((e_time - st) / 1000000)
    println("--------")
    println(kk)
}
