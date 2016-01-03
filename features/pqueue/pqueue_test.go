package pqueue

import (
	"strconv"
	"testing"
	"time"

	"firempq/common"
	"firempq/db"
	"firempq/log"
	"firempq/testutils"

	. "firempq/features/pqueue/pqmsg"
	. "firempq/testutils"

	. "github.com/smartystreets/goconvey/convey"
)

func getConfig() *PQConfig {
	return &PQConfig{
		MaxSize:        100001,
		MsgTtl:         100000,
		DeliveryDelay:  1,
		PopLockTimeout: 10000,
		PopCountLimit:  4,
		LastPushTs:     12,
		LastPopTs:      13,
	}
}

func getDesc() *common.ServiceDescription {
	return &common.ServiceDescription{
		ExportId:  10,
		SType:     "PQueue",
		Name:      "name",
		CreateTs:  123,
		Disabled:  false,
		ToDelete:  false,
		ServiceId: "1",
	}
}

func CreateTestQueue() *PQueue {
	return InitPQueue(getDesc(), getConfig())
}

func CreateNewTestQueue() *PQueue {
	log.InitLogging()
	log.SetLevel(1)
	db.SetDatabase(testutils.NewInMemDBService())
	return CreateTestQueue()
}

func cmp(t *testing.T, a, b string) {
	if a != b {
		t.Error("Unexpected value '" + a + "'. Expecting: '" + b + "'")
	}
}

func TestPushPopAndTimeUnlockItems(t *testing.T) {
	q := CreateNewTestQueue()
	Convey("Test push and pop messages", t, func() {
		q.Push("data1", "p1", 10000, 0, 12)
		q.Push("data2", "p2", 10000, 0, 12)
		VerifyServiceSize(q, 2)
		VerifyItems(q.Pop(10000, 0, 10, true), 2, "data1", "p1", "data2", "p2")
		VerifyServiceSize(q, 2)

		// Unlock item data1 it should become available again.
		q.UpdateLock("data1", 0)
		q.checkTimeouts(common.Uts() + 110)
		VerifySingleItem(q.Pop(10000, 0, 1, true), "data1", "p1")
		VerifyServiceSize(q, 2)

		VerifyOkResponse(q.DeleteLockedById("data1"))

		VerifyServiceSize(q, 1)
		q.DeleteLockedById("data2")
		VerifyServiceSize(q, 0)
	})
}

func TestAutoExpiration(t *testing.T) {
	q := CreateNewTestQueue()
	Convey("Two messages should expire, one message should still be in the queue", t, func() {
		q.Push("data1", "p1", 1000, 0, 12)
		q.Push("data2", "p2", 1000, 0, 12)
		q.Push("data3", "p3", 10000, 0, 12)
		VerifyServiceSize(q, 3)
		q.checkTimeouts(common.Uts() + 1300)
		VerifyServiceSize(q, 1)
	})
}

func TestUnlockById(t *testing.T) {
	q := CreateNewTestQueue()
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
	q := CreateNewTestQueue()
	Convey("Delete not locked message", t, func() {
		q.Push("data1", "p1", 1000, 0, 12)
		VerifyServiceSize(q, 1)
		q.DeleteById("data1")
		VerifyServiceSize(q, 0)
		VerifyItems(q.Pop(10000, 0, 10, true), 0)
	})
}

func TestDeleteLockedById(t *testing.T) {
	q := CreateNewTestQueue()
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
	q := CreateNewTestQueue()
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

func TestPopWaitTimeout(t *testing.T) {
	q := CreateNewTestQueue()
	Convey("Messages should be delivered after 0.1 seconds", t, func() {
		q.Push("d1", "1", 10000, 10000, 12)
		q.Push("d2", "2", 10000, 10000, 12)
		q.Push("d3", "3", 10000, 10000, 12)
		VerifyItems(q.Pop(0, 10, 10, true), 0)
	})
}

func TestDeliveryDelay(t *testing.T) {
	q := CreateNewTestQueue()
	// Stop updates.
	defer q.Close()
	q.StartUpdate()
	Convey("Message delivery delay should be delayed at least for 0.1 seconds", t, func() {
		q.Push("data1", "p1", 10000, 120, 12)
		startTs := time.Now().UnixNano()
		VerifyItems(q.Pop(10000, 1000, 10, true), 1, "data1", "p1")
		finishTs := time.Now().UnixNano()
		So(finishTs-startTs, ShouldBeGreaterThan, time.Second/10)
	})
}

func TestPushLotsOfMessages(t *testing.T) {
	q := CreateNewTestQueue()
	q.StartUpdate()
	totalMsg := 10000
	Convey("10k messages should be pushed and received being removed", t, func() {
		for i := 0; i < totalMsg; i++ {
			q.Push("id"+strconv.Itoa(i), " ", 100000, 0, 10)
		}
		work := true
		go func() {
			time.Sleep(time.Second * 5)
			work = false
		}()
		counter := 0
		loops := 0
		for counter < totalMsg && loops < totalMsg && work {
			resp, ok := q.Pop(0, 10, 10, false).(*common.ItemsResponse)
			if !ok {
				break
			}
			counter += len(resp.GetItems())
		}
		So(counter, ShouldEqual, totalMsg)
		VerifyServiceSize(q, 0)

	})
	q.Close()
}

func TestMessageLoad(t *testing.T) {
	q := CreateNewTestQueue()
	Convey("Push some messages and load them", t, func() {
		q.Push("d1", "p", 100000, 0, 10)
		q.Push("d2", "p", 100000, 0, 11)
		q.Push("d3", "p", 100000, 0, 9)
		q.Push("d4", "p", 100000, 0, 9)
		q.Push("d5", "p", 100000, 0, 9)
		q.Push("d6", "p", 100000, 0, 12)
		q.Push("d0", "p", 100000, 0, 0)

		// This messages should be removed as expired during reload.
		VerifyOkResponse(q.Push("dd", "dd", 0, 0, 20))
		VerifyServiceSize(q, 8)
		// Queue size should be the same since message is just locked.
		VerifySingleItem(q.Pop(1000, 0, 1, true), "d0", "p")
		VerifyServiceSize(q, 8)

		q.Close()

		So(q.IsClosed(), ShouldBeTrue)

		q := CreateTestQueue()
		// 7 messages because one of them has expired during reload.
		VerifyServiceSize(q, 7)
		VerifySingleItem(q.Pop(0, 0, 1, false), "d3", "p")
		VerifySingleItem(q.Pop(0, 0, 1, false), "d4", "p")
		VerifySingleItem(q.Pop(0, 0, 1, false), "d5", "p")
		VerifySingleItem(q.Pop(0, 0, 1, false), "d1", "p")
		VerifySingleItem(q.Pop(0, 0, 1, false), "d2", "p")
		VerifySingleItem(q.Pop(0, 0, 1, false), "d6", "p")
		VerifyServiceSize(q, 1)
		VerifyOkResponse(q.DeleteLockedById("d0"))
		VerifyServiceSize(q, 0)
		q.Clear()
	})
}

func TestStatus(t *testing.T) {
	Convey("Queue status should be correct", t, func() {
		q := CreateNewTestQueue()
		Convey("Empty status should be default", func() {
			s, _ := q.GetCurrentStatus().(*common.DictResponse)
			status := s.GetDict()
			So(status[PQ_STATUS_MAX_SIZE], ShouldEqual, 100001)
			So(status[PQ_STATUS_MSG_TTL], ShouldEqual, 100000)
			So(status[PQ_STATUS_DELIVERY_DELAY], ShouldEqual, 1)
			So(status[PQ_STATUS_POP_LOCK_TIMEOUT], ShouldEqual, 10000)
			So(status[PQ_STATUS_POP_COUNT_LIMIT], ShouldEqual, 4)
			So(status[PQ_STATUS_CREATE_TS], ShouldEqual, 123)
			So(status[PQ_STATUS_LAST_PUSH_TS], ShouldEqual, 12)
			So(status[PQ_STATUS_LAST_POP_TS], ShouldEqual, 13)
			So(status[PQ_STATUS_TOTAL_MSGS], ShouldEqual, 0)
			So(status[PQ_STATUS_IN_FLIGHT_MSG], ShouldEqual, 0)
			So(status[PQ_STATUS_AVAILABLE_MSGS], ShouldEqual, 0)

			So(q.GetServiceId(), ShouldEqual, "1")
			So(q.GetTypeName(), ShouldEqual, common.STYPE_PRIORITY_QUEUE)
		})
		Convey("Status for several messages in flight", func() {
			q.Push("d1", "p", 10000, 0, 9)
			q.Push("d2", "p", 10000, 0, 9)
			q.Push("d3", "p", 10000, 0, 9)

			VerifySingleItem(q.Pop(100000, 0, 1, true), "d1", "p")
			VerifyServiceSize(q, 3)

			s, _ := q.GetCurrentStatus().(*common.DictResponse)
			status := s.GetDict()
			So(status[PQ_STATUS_MAX_SIZE], ShouldEqual, 100001)
			So(status[PQ_STATUS_MSG_TTL], ShouldEqual, 100000)
			So(status[PQ_STATUS_DELIVERY_DELAY], ShouldEqual, 1)
			So(status[PQ_STATUS_POP_LOCK_TIMEOUT], ShouldEqual, 10000)
			So(status[PQ_STATUS_POP_COUNT_LIMIT], ShouldEqual, 4)
			So(status[PQ_STATUS_CREATE_TS], ShouldBeLessThanOrEqualTo, common.Uts())
			So(status[PQ_STATUS_LAST_PUSH_TS], ShouldBeLessThanOrEqualTo, common.Uts())
			So(status[PQ_STATUS_LAST_POP_TS], ShouldBeLessThanOrEqualTo, common.Uts())
			So(status[PQ_STATUS_TOTAL_MSGS], ShouldEqual, 3)
			So(status[PQ_STATUS_IN_FLIGHT_MSG], ShouldEqual, 1)
			So(status[PQ_STATUS_AVAILABLE_MSGS], ShouldEqual, 2)
		})

	})
}

func TestSetParams(t *testing.T) {
	Convey("Parameters should be set", t, func() {
		q := CreateNewTestQueue()
		VerifyOkResponse(q.SetParams(10000, 20000, 30000, 40000, 50000))

		s, _ := q.GetCurrentStatus().(*common.DictResponse)
		status := s.GetDict()
		So(status[PQ_STATUS_MSG_TTL], ShouldEqual, 10000)
		So(status[PQ_STATUS_MAX_SIZE], ShouldEqual, 20000)
		So(status[PQ_STATUS_DELIVERY_DELAY], ShouldEqual, 30000)
		So(status[PQ_STATUS_POP_COUNT_LIMIT], ShouldEqual, 40000)
		So(status[PQ_STATUS_POP_LOCK_TIMEOUT], ShouldEqual, 50000)
	})
}

func TestGetMessageInfo(t *testing.T) {
	Convey("Pushed message should return parameters", t, func() {
		q := CreateNewTestQueue()
		q.Push("d1", "p", 10000, 1000, 9)
		q.Push("d2", "p", 10000, 0, 11)

		So(q.GetMessageInfo("d3"), ShouldResemble, common.ERR_MSG_NOT_FOUND)

		m1, _ := q.GetMessageInfo("d1").(*common.DictResponse)
		m2, _ := q.GetMessageInfo("d2").(*common.DictResponse)

		msgInfo1 := m1.GetDict()
		msgInfo2 := m2.GetDict()

		So(msgInfo1[MSG_INFO_ID], ShouldEqual, "d1")
		So(msgInfo1[MSG_INFO_LOCKED], ShouldEqual, true)
		So(msgInfo1[MSG_INFO_UNLOCK_TS], ShouldBeGreaterThan, common.Uts())
		So(msgInfo1[MSG_INFO_POP_COUNT], ShouldEqual, 0)
		So(msgInfo1[MSG_INFO_PRIORITY], ShouldEqual, 9)
		So(msgInfo1[MSG_INFO_EXPIRE_TS], ShouldBeGreaterThan, common.Uts())

		So(msgInfo2[MSG_INFO_ID], ShouldEqual, "d2")
		So(msgInfo2[MSG_INFO_LOCKED], ShouldEqual, false)
		So(msgInfo2[MSG_INFO_UNLOCK_TS], ShouldBeLessThanOrEqualTo, common.Uts())
		So(msgInfo2[MSG_INFO_POP_COUNT], ShouldEqual, 0)
		So(msgInfo2[MSG_INFO_PRIORITY], ShouldEqual, 11)
		So(msgInfo2[MSG_INFO_EXPIRE_TS], ShouldBeGreaterThan, common.Uts())

	})
}

func TestUnlockErrors(t *testing.T) {
	Convey("Attempts to unlock not locked and not existing messages should result in error", t, func() {
		q := CreateNewTestQueue()
		q.Push("d1", "p", 10000, 0, 11)
		So(q.UnlockMessageById("d1"), ShouldResemble, common.ERR_MSG_NOT_LOCKED)
		So(q.UnlockMessageById("d2"), ShouldResemble, common.ERR_MSG_NOT_FOUND)
	})
}

func TestDeleteMessageErrors(t *testing.T) {
	Convey("Attempts to delete locked and not existing message should result in error", t, func() {
		q := CreateNewTestQueue()
		q.Push("d1", "p", 10000, 100, 11)
		So(q.DeleteById("d1"), ShouldResemble, common.ERR_MSG_IS_LOCKED)
		So(q.DeleteById("d2"), ShouldResemble, common.ERR_MSG_NOT_FOUND)
	})
}

func TestPushError(t *testing.T) {
	q := CreateNewTestQueue()
	q.Push("d1", "p", 10000, 100, 11)
	Convey("Push should result in errors", t, func() {
		So(q.Push("d1", "p", 10000, 100, 1), ShouldResemble, common.ERR_ITEM_ALREADY_EXISTS)
	})
}

func TestExpiration(t *testing.T) {
	Convey("One item should expire", t, func() {
		q := CreateNewTestQueue()
		q.Push("d1", "p", 10000, 0, 11)
		r, _ := q.TimeoutItems(common.Uts() + 100000).(*common.IntResponse)
		So(r.Value, ShouldEqual, 1)
		VerifyServiceSize(q, 0)
	})
}

func TestReleaseInFlight(t *testing.T) {
	Convey("One item should expire", t, func() {
		q := CreateNewTestQueue()
		q.Push("d1", "p", 10000, 100, 11)
		r, _ := q.ReleaseInFlight(common.Uts() + 1000).(*common.IntResponse)
		So(r.Value, ShouldEqual, 1)
		VerifySingleItem(q.Pop(0, 0, 1, false), "d1", "p")
	})
}

func TestPopCountExpiration(t *testing.T) {
	Convey("Item should disappear on fifth attempt to pop it", t, func() {
		q := CreateNewTestQueue()
		q.Push("d1", "p", 10000, 0, 1)
		VerifySingleItem(q.Pop(0, 0, 1, true), "d1", "p")
		VerifyOkResponse(q.UnlockMessageById("d1"))

		VerifySingleItem(q.Pop(0, 0, 1, true), "d1", "p")
		VerifyOkResponse(q.UnlockMessageById("d1"))

		VerifySingleItem(q.Pop(0, 0, 1, true), "d1", "p")
		VerifyOkResponse(q.UnlockMessageById("d1"))

		VerifySingleItem(q.Pop(0, 0, 1, true), "d1", "p")
		VerifyOkResponse(q.UnlockMessageById("d1"))

		// PopAttempts end here.
		VerifyItemsResponse(q.Pop(0, 0, 1, true), 0)

		VerifyServiceSize(q, 0)
	})
}

func TestSize(t *testing.T) {
	Convey("Size of different structures should be valid", t, func() {
		q := CreateNewTestQueue()
		q.Push("d1", "p", 10000, 0, 11)
		q.Push("d2", "p", 10000, 0, 11)
		q.Push("d3", "p", 10000, 0, 11)
		q.Push("d4", "p", 10000, 0, 11)
		q.Push("d5", "p", 10000, 0, 11)
		q.Pop(0, 0, 2, true)

		VerifyServiceSize(q, 5)
		So(q.availMsgs.Len(), ShouldEqual, 3)
		So(q.lockedMsgCnt, ShouldEqual, 2)
		So(len(q.id2sn), ShouldEqual, 5)
	})
}

func TestSizeLimit(t *testing.T) {
	Convey("Fourth element should fail with size limit error", t, func() {
		q := CreateNewTestQueue()
		q.SetParams(10000, 3, 10000, 0, 50000)
		VerifyOkResponse(q.Push("1", "p", 10000, 0, 11))
		VerifyOkResponse(q.Push("2", "p", 10000, 0, 11))
		VerifyOkResponse(q.Push("3", "p", 10000, 0, 11))
		So(q.Push("4", "p", 10000, 0, 11), ShouldResemble, common.ERR_SIZE_EXCEEDED)
		VerifyServiceSize(q, 3)
	})
}
