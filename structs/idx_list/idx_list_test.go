package structs

import (
	"strconv"
	"testing"
)

func TestListQueue1(t *testing.T) {

	lq := NewStrIndexList()

	for i := 0; i < 1000; i++ {
		lq.PushBack(strconv.Itoa(i))
	}

	for i := 0; i < 1000; i++ {
		if lq.PopFront() != strconv.Itoa(i) {
			t.Error("Unexpected queue item")
		}
	}
}

func TestListQueue2(t *testing.T) {

	lq := NewStrIndexList()

	for i := 0; i < 1000; i++ {
		lq.PushBack(strconv.Itoa(i))
	}

	for i := 0; i < 1000; i++ {
		k := strconv.Itoa(i)
		if !lq.RemoveById(k) {
			t.Error("Elemen not found!")
		}
	}

	for i := 0; i < 1000; i++ {
		k := strconv.Itoa(i)
		if lq.RemoveById(k) {
			t.Error("Elemen has been found!")
		}
	}
}

func TestListQueue3(t *testing.T) {

	lq := NewStrIndexList()

	for i := 0; i < 1000; i++ {
		lq.PushFront(strconv.Itoa(i))
	}

	for i := 0; i < 1000; i++ {
		if lq.PopBack() != strconv.Itoa(i) {
			t.Error("Unexpected queue item")
		}
	}
}
