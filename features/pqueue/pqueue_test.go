package pqueue

import (
	"firempq/common"
	"firempq/db"
	"firempq/log"
	"firempq/testutils"
	"testing"

	. "firempq/testutils"

	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func CreateTestQueue() *PQueue {
	log.InitLogging()
	log.SetLevel(1)
	db.SetDatabase(testutils.NewInMemDBService())
	desc := common.NewServiceDescription(common.STYPE_PRIORITY_QUEUE, 54634673456, "name")
	return NewPQueue(desc, 100, 1000)
}

func cmp(t *testing.T, a, b string) {
	if a != b {
		t.Error("Unexpected value '" + a + "'. Expecting: '" + b + "'")
	}
}

func TestPushPopAndTimeUnlockItems(t *testing.T) {
	q := CreateTestQueue()
	defer q.Clear()
	Convey("Test push and pop messages", t, func() {
		q.Push("data1", "p1", 10000, 0, 12)
		q.Push("data2", "p2", 10000, 0, 12)
		VerifyServiceSize(q, 2)
		VerifyItems(q.Pop(10000, 0, 10, true), 2, "data1", "p1", "data2", "p2")
		VerifyServiceSize(q, 2)

		// Unlock item data1 it should become available again.
		q.UpdateLock("data1", 0)
		q.update(common.Uts() + 110)
		VerifySingleItem(q.Pop(10000, 0, 1, true), "data1", "p1")
		VerifyServiceSize(q, 2)

		q.DeleteLockedById("data1")
		VerifyServiceSize(q, 1)
		q.DeleteLockedById("data2")
		VerifyServiceSize(q, 0)
	})
}

func TestAutoExpiration(t *testing.T) {
	q := CreateTestQueue()
	defer q.Clear()
	Convey("Two messages should expire, one message should still be in the queue", t, func() {
		q.Push("data1", "p1", 1000, 0, 12)
		q.Push("data2", "p2", 1000, 0, 12)
		q.Push("data3", "p3", 10000, 0, 12)
		VerifyServiceSize(q, 3)
		q.update(common.Uts() + 1300)
		VerifyServiceSize(q, 1)
	})
}

func TestUnlockById(t *testing.T) {
	q := CreateTestQueue()
	defer q.Clear()
	Convey("Locked message should become visible again after it gets unlocked", t, func() {
		q.Push("data1", "p1", 1000, 0, 12)
		q.Push("data2", "p2", 1000, 0, 12)
		VerifyItems(q.Pop(10000, 0, 10, true), 2, "data1", "p1", "data2", "p2")
		VerifyItems(q.Pop(10000, 0, 10, true), 0)
		q.UnlockMessageById("data2")
		VerifyItems(q.Pop(10000, 0, 10, true), 1, "data2", "p2")
	})
}

func TestDeleteById(t *testing.T) {
	q := CreateTestQueue()
	defer q.Clear()
	Convey("Delete not locked message", t, func() {
		q.Push("data1", "p1", 1000, 0, 12)
		VerifyServiceSize(q, 1)
		q.DeleteById("data1")
		VerifyServiceSize(q, 0)
		VerifyItems(q.Pop(10000, 0, 10, true), 0)
	})
}

func TestDeleteLockedById(t *testing.T) {
	q := CreateTestQueue()
	defer q.Clear()
	Convey("Locked message should be removed", t, func() {
		q.Push("data1", "p1", 10000, 0, 12)
		q.Push("data2", "p2", 10000, 0, 12)
		VerifyItems(q.Pop(10000, 0, 10, true), 2, "data1", "p1", "data2", "p2")
		q.DeleteLockedById("data1")
		q.DeleteLockedById("data2")
		VerifyServiceSize(q, 0)
	})
}

func TestPopWaitBatch(t *testing.T) {
	q := CreateTestQueue()
	go func() {
		time.Sleep(time.Second / 10)
		q.Push("d1", "1", 10000, 0, 12)
		q.Push("d2", "2", 10000, 0, 12)
		q.Push("d3", "3", 10000, 0, 12)
	}()
	Convey("Messages should be delivered after 0.1 seconds", t, func() {
		VerifyItems(q.Pop(10000, 1000, 10, true), 3, "d1", "1", "d2", "2", "d3", "3")
	})
}

func TestDeliveryDelay(t *testing.T) {
	q := CreateTestQueue()
	q.StartUpdate()
	Convey("Message delivery delay should be delayed at least for 0.1 seconds", t, func() {
		q.Push("data1", "p1", 10000, 120, 12)
		startTs := time.Now().Nanosecond()
		VerifyItems(q.Pop(10000, 1000, 10, true), 1, "data1", "p1")
		finishTs := time.Now().Nanosecond()
		So(finishTs-startTs, ShouldBeGreaterThan, time.Second/10)
	})
	q.Close()
}
