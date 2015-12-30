package structs

import "testing"

func TestUintListQueue1(t *testing.T) {

	lq := NewUint64IndexList()
	var i uint64
	for i = 0; i < 1000; i++ {
		lq.PushBack(i)
	}

	for i = 0; i < 1000; i++ {
		if lq.PopFront() != i {
			t.Error("Unexpected queue item")
		}
	}
}

func TestUintListQueue2(t *testing.T) {

	lq := NewUint64IndexList()

	var i uint64
	for i = 0; i < 1000; i++ {
		lq.PushBack(i)
	}

	for i = 0; i < 1000; i++ {
		if !lq.RemoveById(i) {
			t.Error("Elemen not found!")
		}
	}

	for i = 0; i < 1000; i++ {
		if lq.RemoveById(i) {
			t.Error("Elemen has been found!")
		}
	}
}

func TestUintListQueue3(t *testing.T) {

	lq := NewUint64IndexList()

	var i uint64
	for i = 0; i < 1000; i++ {
		lq.PushFront(i)
	}

	for i = 0; i < 1000; i++ {
		if lq.PopBack() != i {
			t.Error("Unexpected queue item")
		}
	}
}
