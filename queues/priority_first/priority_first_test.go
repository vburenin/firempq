package priority_first

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestActiveQueue1(t *testing.T) {
	aq := NewActiveQueues(10)

	if aq.Push("id1", 9) != nil {
		t.Error("Should be nil")
	}
	if aq.Push("id2", 9) != nil {
		t.Error("Should be nil")
	}
	if aq.Push("id3", 9) != nil {
		t.Error("Should be nil")
	}
	if aq.Push("id4", 1) != nil {
		t.Error("Should be nil")
	}
	if aq.Push("id5", 0) != nil {
		t.Error("Should be nil")
	}
	if aq.Push("id6", 3) != nil {
		t.Error("Should be nil")
	}
	if aq.Push("id7", 8) != nil {
		t.Error("Should be nil")
	}

	if aq.Pop() != "id5" {
		t.Error("Expected id5!")
	}
	if aq.Pop() != "id4" {
		t.Error("Expected id4!")
	}
	if aq.Pop() != "id6" {
		t.Error("Expected id6!")
	}
	if aq.Pop() != "id7" {
		t.Error("Expected id7!")
	}
	if aq.Pop() != "id1" {
		t.Error("Expected id1!")
	}
	if aq.Pop() != "id2" {
		t.Error("Expected id2!")
	}
	if aq.Pop() != "id3" {
		t.Error("Expected id3!")
	}

}

func TestRandomItems(t *testing.T) {
	border := 10
	history := make([][]string, border)
	aq := NewActiveQueues(border)
	for i := 0; i < 100000; i++ {
		priority := rand.Intn(border)
		itemId := strconv.Itoa(rand.Int())
		history[priority] = append(history[priority], itemId)
		aq.Push(itemId, priority)
	}

	prevp := 0
	for !aq.Empty() {
		p := aq.getFirstAvailable()
		if p >= prevp {
			prevp = p
		} else {
			t.Error("Priority is higher then the previous one!!")
		}

		expected_item := history[p][0]
		history[p] = history[p][1:]
		if expected_item != aq.Pop() {
			t.Error("Wrong expected item!")
		}
	}

}
