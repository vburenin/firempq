package dsqueue

import (
	"firempq/common"
	"firempq/db"
	"strconv"
	"testing"

	. "firempq/testutils"

	. "github.com/smartystreets/goconvey/convey"
)

func CreateTestQueue() *DSQueue {
	desc := common.NewServiceDescription(common.STYPE_DOUBLE_SIDED_QUEUE, 15243523452345, "dsqueue-test")
	return NewDSQueue(desc, 1000)
}

func newItem(itemId, payload string) []string {
	return []string{PRM_ID, itemId, PRM_PAYLOAD, payload}
}

func newDelayItem(itemId, payload string, delay int) []string {
	return []string{PRM_ID, itemId, PRM_PAYLOAD, payload, PRM_DELAY, strconv.Itoa(delay)}
}

func itemId(itemId string) []string {
	return []string{PRM_ID, itemId}
}

func TestPopLockDelete(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()
	Convey("Poplock and delete messages should work", t, func() {

		Convey("Two messages should be pushed into the queue", func() {
			VerifyOk(q.PushFront(newItem("data1", "p1")))
			VerifyOk(q.PushFront(newItem("data2", "p2")))
			VerifySize(q, 2)
		})

		Convey("Pop and lock one message from the front of the queue and try to delete it", func() {
			VerifyItem(q.PopLockFront(nil), "data2", "p2")
			VerifyError(q.DeleteById(itemId("data2")))
			VerifyOk(q.DeleteLockedById(itemId("data2")))
		})

		Convey("Make sure queue contains one element", func() {
			VerifySize(q, 1)
		})

		Convey("Poplock and delete last message", func() {
			VerifyItem(q.PopLockFront(nil), "data1", "p1")
			VerifyOk(q.DeleteLockedById(itemId("data1")))
		})
		Convey("Make sure queue is empty now", func() {
			VerifySize(q, 0)
		})
	})
}

func TestPushAndPopFromFront(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()
	Convey("Data should be pushed and popped in stack order", t, func() {

		Convey("Messages should be pushed", func() {
			VerifyOk(q.PushFront(newItem("data1", "p1")))
			VerifyOk(q.PushFront(newItem("data2", "p2")))
		})

		Convey("Messages should be popped in LIFO order", func() {
			VerifyItem(q.PopFront(nil), "data2", "p2")
			VerifyItem(q.PopFront(nil), "data1", "p1")
		})
	})
}

func TestPushFrontDelayed(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	Convey("Test message delivered with delay", t, func() {

		Convey("Messages should be pushed", func() {
			VerifyOk(q.PushFront(newItem("data1", "p1")))
			VerifyOk(q.PushFront(newDelayItem("data2", "p2", 100)))
		})

		Convey("data1 should be first because of delay", func() {
			q.update(common.Uts() + 50)
			VerifyItem(q.PopFront(nil), "data1", "p1")
			q.update(common.Uts() + 30)
			VerifyItemsResponse(q.PopFront(nil), 0)
			q.update(common.Uts() + 250)
			VerifyItem(q.PopFront(nil), "data2", "p2")
		})
	})
}

func TestPushAndPopFromBack(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	Convey("Data should be pushed and popped in stack order", t, func() {

		Convey("Messages should be pushed to the back side", func() {
			VerifyOk(q.PushBack(newItem("data1", "p1")))
			VerifyOk(q.PushBack(newItem("data2", "p2")))
		})

		Convey("Messages should be popped in LIFO order from back", func() {
			VerifyItem(q.PopBack(nil), "data2", "p2")
			VerifyItem(q.PopBack(nil), "data1", "p1")
		})
	})
}

func TestPushBackDelayed(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	Convey("Test message delivered with delay", t, func() {

		Convey("Messages should be pushed", func() {
			VerifyOk(q.PushBack(newItem("data1", "p1")))
			VerifyOk(q.PushBack(newDelayItem("data2", "p2", 100)))
		})

		Convey("data1 should be first because of delay", func() {
			q.update(common.Uts() + 50)
			VerifyItem(q.PopBack(nil), "data1", "p1")
			q.update(common.Uts() + 30)
			VerifyItemsResponse(q.PopBack(nil), 0)
			q.update(common.Uts() + 250)
			VerifyItem(q.PopBack(nil), "data2", "p2")
		})
	})
}

func TestAutoExpiration(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()
	q.conf.MsgTtl = 10

	Convey("Test message expiration", t, func() {
		Convey("Messages should be pushed", func() {
			VerifyOk(q.PushBack(newItem("data1", "p1")))
			VerifyOk(q.PushFront(newItem("data3", "p3")))
			VerifyOk(q.PushBack(newDelayItem("data2", "p2", 9)))
			VerifySize(q, 3)
		})
		Convey("Expire messages", func() {
			So(q.update(common.Uts()+2000), ShouldBeTrue)
		})
		Convey("Nothing should left", func() {
			VerifySize(q, 0)
			VerifyItemsResponse(q.PopFront(nil), 0)
			VerifyItemsResponse(q.PopBack(nil), 0)
		})
	})
}

func TestLockExpiration(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	Convey("Test message lock expiration", t, func() {
		Convey("Messages should be pushed", func() {
			VerifyOk(q.PushBack(newItem("data1", "p1")))
			VerifyOk(q.PushBack(newItem("data2", "p2")))
			VerifySize(q, 2)
		})

		Convey("Messages should be popped and locked", func() {
			VerifyItem(q.PopLockFront(nil), "data1", "p1")
			VerifyItem(q.PopLockFront(nil), "data2", "p2")
			VerifySize(q, 2)
		})

		Convey("Expire message lock", func() {
			So(q.update(common.Uts()+2000), ShouldBeTrue)
		})
		Convey("Nothing should left", func() {
			VerifySize(q, 2)
			VerifyItem(q.PopFront(nil), "data1", "p1")
			VerifyItem(q.PopFront(nil), "data2", "p2")
			VerifySize(q, 0)
		})
	})
}

func TestLoadDataFromDatabase(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	Convey("Push different items into database", t, func() {
		VerifyOk(q.PushBack(newItem("d1", "p1")))
		VerifyOk(q.PushFront(newItem("d2", "p1")))
		VerifyOk(q.PushBack(newItem("d3", "p1")))
		VerifyOk(q.PushFront(newItem("d4", "p1")))
		VerifyOk(q.PushBack(newItem("d5", "p1")))
		VerifyOk(q.PushFront(newItem("d6", "p1")))
		VerifyOk(q.PushBack(newItem("d7", "p1")))
		VerifySize(q, 7)
	})
	q.Close()
	db.GetDatabase().Close()

	q = CreateTestQueue()
	Convey("Data should be loaded correctly", t, func() {
		VerifySize(q, 7)
		VerifyItem(q.PopFront(nil), "d6", "p1")
		VerifyItem(q.PopFront(nil), "d4", "p1")
		VerifyItem(q.PopFront(nil), "d2", "p1")
		VerifyItem(q.PopFront(nil), "d1", "p1")
		VerifyItem(q.PopFront(nil), "d3", "p1")
		VerifyItem(q.PopFront(nil), "d5", "p1")
		VerifyItem(q.PopFront(nil), "d7", "p1")
	})
	q.Clear()
	q.Close()
}

func TestGetStatus(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	q.conf.MsgTtl = 100000
	q.conf.DeliveryDelay = 0
	q.conf.PopLockTimeout = 1000
	q.conf.PopCountLimit = 2
	q.conf.MaxSize = 100
	q.desc.CreateTs = 2000

	defer q.Close()
	defer q.Clear()

	Convey("Test Queue Status", t, func() {
		Convey("Messages should be pushed", func() {
			VerifyOk(q.PushBack(newItem("data1", "p1")))
			VerifyOk(q.PushBack(newItem("data2", "p2")))
			VerifyItem(q.PopLockFront(nil), "data1", "p1")
			VerifySize(q, 2)
		})

		Convey("Messages should be popped and locked", func() {
			data := q.GetCurrentStatus(nil).(*common.DictResponse).GetDict()
			So(data["MsgTtl"], ShouldEqual, 100000)
			So(data["DeliveryDelay"], ShouldEqual, 0)
			So(data["PopLockTimeout"], ShouldEqual, 1000)
			So(data["PopCountLimit"], ShouldEqual, 2)
			So(data["MaxSize"], ShouldEqual, 100)
			So(data["CreateTs"], ShouldEqual, 2000)
			So(data["TotalMessages"], ShouldEqual, 2)
			So(data["InFlightSize"], ShouldEqual, 1)
			So(data["LastPushTs"], ShouldBeGreaterThan, 100000)
			So(data["LastPopTs"], ShouldBeGreaterThan, 100000)
		})
	})
}

//func TestLoadFromDb(t *testing.T) {
//	common.EnableTesting()
//	defer common.DisableTesting()
//	q := CreateTestQueue()
//	q.Clear()
//	defer q.Close()
//	defer q.Clear()
//
//	q.PushBack([]string{PRM_ID, "b1", PRM_PAYLOAD, "p1"})
//	q.PushBack([]string{PRM_ID, "b2", PRM_DELAY, "500", PRM_PAYLOAD, "p2"})
//	q.PushFront([]string{PRM_ID, "f1", PRM_DELAY, "500", PRM_PAYLOAD, "p1"})
//	q.PushFront([]string{PRM_ID, "f2", PRM_PAYLOAD, "p1"})
//	q.PushFront([]string{PRM_ID, "f3", PRM_PAYLOAD, "p1"})
//	msg := q.PopLockFront(nil).GetResponse()
//
//	if !strings.Contains(msg, "f3") {
//		t.Error("Invalid message, expecterdf")
//	}
//
//	q.SetLockTimeout([]string{PRM_ID, "f3", PRM_LOCK_TIMEOUT, "100"})
//	// Wait till f3 will be unlocked and returned to the queue (priority front)
//	common.IncTimer(120)
//	q.update(common.Uts())
//
//	q.Close()
//
//	// Now reload queue from db as a new instance (should contain f3, f2, b1)
//	ql := CreateTestQueue()
//	defer ql.Close()
//	defer ql.Clear()
//
//	if len(ql.allMessagesMap) != 5 {
//		t.Error("5 messages should be in the queue")
//	}
//
//	if ql.availableMsgs.Len() != 2 {
//		t.Error("2 messages should be available in generic queue", ql.availableMsgs.Len())
//	}
//
//	if ql.highPriorityFrontMsgs.Len() != 1 {
//		t.Error("1 message should be in high priority front queue")
//	}
//
//	if ql.inFlightHeap.Len() != 2 {
//		t.Error("Should be 2 messages in flight")
//	}
//	common.IncTimer(1600)
//	ql.update(common.Uts())
//
//	// Now f1 and b2 delivered and queue should contain 5 messages)
//	if ql.availableMsgs.Len()+ql.highPriorityFrontMsgs.Len() != 5 {
//		t.Error("Messages map should contain 5 messages instead of ", ql.availableMsgs.Len())
//	}
//	// Check order of loaded messages (front)
//	msg = ql.PopLockFront(nil).GetResponse()
//
//	if !strings.Contains(msg, "f3") {
//		t.Error("Messages order is wrong! Got", msg, "instead of f3")
//	}
//	msg = ql.PopLockFront(nil).GetResponse()
//	if !strings.Contains(msg, "f1") {
//		t.Error("Messages order is wrong! Got", msg, "instead of f1")
//	}
//	msg = ql.PopLockFront(nil).GetResponse()
//	if !strings.Contains(msg, "f2") {
//		t.Error("Messages order is wrong! Got", msg, "instead of f2")
//	}
//
//	// Check order of loaded messages (back)
//	msg = ql.PopLockBack(nil).GetResponse()
//	if !strings.Contains(msg, "b2") {
//		t.Error("Messages order is wrong! Got", msg, "instead of b2")
//	}
//	msg = ql.PopLockBack(nil).GetResponse()
//	if !strings.Contains(msg, "b1") {
//		t.Error("Messages order is wrong! Got", msg, "instead of b1")
//	}
//}
