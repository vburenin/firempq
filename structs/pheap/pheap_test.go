package pheap

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPushAndPop(t *testing.T) {
	Convey("Items should be poped in sorted order", t, func() {
		h := NewPHeap()
		for i := 1000000; i >= 0; i-- {
			h.Push(int64(i), uint64(i))
		}
		failed := false
		for i := 0; i <= 1000000; i++ {
			if uint64(i) != h.Pop().IntId {
				failed = true
				break
			}
		}
		So(failed, ShouldBeFalse)
	})
}

func TestUnorderedPushAndPop(t *testing.T) {
	Convey("Items should be poped in sorted order", t, func() {
		h := NewPHeap()
		h.Push(0, 12)
		h.Push(0, 100)
		h.Push(1, 25)
		h.Push(1, 38)
		h.Push(2, 11)
		So(h.Pop().IntId, ShouldEqual, 12)
		h.Push(0, 11)
		So(h.Pop().IntId, ShouldEqual, 11)
		h.Push(10, 0)

		So(len(h.data), ShouldEqual, 4)
		So(len(h.index), ShouldEqual, 4)

		So(h.Pop().IntId, ShouldEqual, 100)
		So(h.Pop().IntId, ShouldEqual, 25)
		So(h.Pop().IntId, ShouldEqual, 38)
		So(h.Pop().IntId, ShouldEqual, 0)
	})
}

func TestUnorderedPushAndPopWithRemove(t *testing.T) {
	Convey("Items should be poped in sorted order", t, func() {
		h := NewPHeap()
		h.Push(0, 12)
		h.Push(0, 100)
		h.Push(1, 25)
		h.Push(1, 38)
		h.Push(2, 11)

		h.Remove(100)

		So(h.Pop().IntId, ShouldEqual, 12)

		h.Remove(38)

		h.Push(0, 11)

		So(h.Pop().IntId, ShouldEqual, 11)

		h.Push(10, 0)

		So(h.Pop().IntId, ShouldEqual, 25)
		So(h.Pop().IntId, ShouldEqual, 0)
		So(len(h.data), ShouldEqual, 0)
		So(len(h.index), ShouldEqual, 0)
	})
}
