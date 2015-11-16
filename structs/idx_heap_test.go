package structs

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"runtime"

	"github.com/golangplus/testing/assert"
)

//func TestHeap1(t *testing.T) {
//	st := time.Now().UnixNano()
//	mh := NewIndexHeap()
//
//	var i int
//	for i = 0; i < 100; i++ {
//		mh.PushItem(strconv.Itoa(i), int64(i))
//	}
//
//	e_time := time.Now().UnixNano()
//	println((e_time - st) / 1000000)
//	println("--------")
//
//	st = time.Now().UnixNano()
//
//	kk := 0
//	for mh.Len() > 0 {
//		mh.PopItem()
//		kk += 1
//	}
//	e_time = time.Now().UnixNano()
//	println((e_time - st) / 1000000)
//	println("--------")
//	println(kk)
//}

func TestPriorityQueue1(t *testing.T) {
	pq := NewIndexedPriorityQueue()
	pq.PushItem("1", 1)
	pq.PushItem("2", 2)
	pq.PushItem("3", 3)
	item := pq.PopItem()
	assert.Equal(t, "item.Priority", item.Priority, int64(1))
	assert.Equal(t, "item.Id", item.Id, "1")
	item = pq.PopItem()
	assert.Equal(t, "item.Priority", item.Priority, int64(2))
	assert.Equal(t, "item.Id", item.Id, "2")
	item = pq.PopItem()
	assert.Equal(t, "item.Priority", item.Priority, int64(3))
	assert.Equal(t, "item.Id", item.Id, "3")
	assert.Equal(t, "iHeap", len(pq.iHeap), 0)
	assert.Equal(t, "iHeap", len(pq.iMap), 0)
}

func TestPriorityQueue2(t *testing.T) {
	pq := NewIndexedPriorityQueue()
	pq.PushItem("1", 1)
	pq.PushItem("2", 2)
	pq.PushItem("3", 3)

	item := pq.PopById("2")
	assert.Equal(t, "item.Priority", item.Priority, int64(2))
	assert.Equal(t, "item.Id", item.Id, "2")

	assert.Equal(t, "iHeap", len(pq.iHeap), 2)
	assert.Equal(t, "iHeap", len(pq.iMap), 2)

	item = pq.PopItem()
	assert.Equal(t, "item.Priority", item.Priority, int64(1))
	assert.Equal(t, "item.Id", item.Id, "1")

	item = pq.PopItem()
	assert.Equal(t, "item.Priority", item.Priority, int64(3))
	assert.Equal(t, "item.Id", item.Id, "3")
}

func TestPriorityQueue3(t *testing.T) {
	pq := NewIndexedPriorityQueue()
	assert.True(t, "NewItem", pq.PushItem("1", 1))
	assert.True(t, "NewItem", pq.PushItem("2", 2))
	assert.True(t, "NewItem", pq.PushItem("3", 3))
	assert.False(t, "NewItem", pq.PushItem("3", 3))
}

func TestPriorityQueue4(t *testing.T) {
	pq := NewIndexedPriorityQueue()
	pq.PushItem("data1", 12)
	pq.PushItem("data2", 12)
	item := pq.PopById("data1")
	assert.Equal(t, "item.Priority", item.Priority, int64(12))
	assert.Equal(t, "item.Id", item.Id, "data1")
	item = pq.PopById("data2")
	assert.Equal(t, "item.Priority", item.Priority, int64(12))
	assert.Equal(t, "item.Id", item.Id, "data2")
}

func test() float64 {
	data := []HeapItem{}
	idxHeap := NewIndexedPriorityQueue()

	rand.Seed(0)

	for i := 0; i < 1000; i++ {
		v := rand.Int()
		data = append(data, HeapItem{strconv.Itoa(v), int64(v)})
	}
	for _, hi := range data {
		idxHeap.PushItem(hi.Id, hi.Priority)
	}
	runtime.GC()
	st := time.Now().UnixNano()
	for idxHeap.Len() > 0 {
		idxHeap.PopItem()
	}

	return float64(time.Now().UnixNano()-st) / 1000000.0
}

func TestHeap2(t *testing.T) {
	best := test()
	for i := 0; i < 100; i++ {
		c := test()
		if c < best {
			best = c
		}
	}
	fmt.Printf("PriorityQueue: %f\n", best)
}

func TestHeap3(t *testing.T) {
	data := []HeapItem{}
	idxHeap := NewIndexHeap()

	rand.Seed(0)

	for i := 0; i < 1000; i++ {
		v := rand.Int()
		data = append(data, HeapItem{strconv.Itoa(v), int64(v)})
	}
	st := time.Now().UnixNano()
	for _, hi := range data {
		idxHeap.PushItem(hi.Id, hi.Priority)
	}
	f := float64(time.Now().UnixNano()-st) / 1000000000.0
	fmt.Printf("IndexHeap: %f\n", f)
}
