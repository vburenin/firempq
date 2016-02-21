package pqueue

import (
	"strconv"
	"testing"
	"time"

	"firempq/db"
	"firempq/log"
	"firempq/testutils"

	. "firempq/api"
	. "firempq/common"
	. "firempq/conf"
	. "firempq/errors"
	. "firempq/response"
	. "firempq/services/pqueue/pqmsg"
	. "firempq/services/svcmetadata"
	. "firempq/testutils"
	. "firempq/utils"

	. "github.com/smartystreets/goconvey/convey"
)

func getConfig() *PQConfig {
	return &PQConfig{
		MaxMsgsInQueue: 100001,
		MaxMsgSize:     256000,
		MsgTtl:         100000,
		DeliveryDelay:  1,
		PopLockTimeout: 10000,
		PopCountLimit:  4,
		LastPushTs:     12,
		LastPopTs:      13,
	}
}

func getDesc() *ServiceDescription {
	return &ServiceDescription{
		ExportId:  10,
		SType:     "PQueue",
		Name:      "name",
		CreateTs:  123,
		Disabled:  false,
		ToDelete:  false,
		ServiceId: "1",
	}
}

type FakeSvcLoader struct {
	data map[string]*PQueue
}

func NewFakeSvcLoader() *FakeSvcLoader {
	return &FakeSvcLoader{
		data: make(map[string]*PQueue),
	}
}

func (f *FakeSvcLoader) GetService(name string) (ISvc, bool) {
	svc, ok := f.data[name]
	return svc, ok
}

func CreateTestQueue() *PQueue {
	return InitPQueue(NewFakeSvcLoader(), getDesc(), getConfig())
}

func CreateTestQueueWithName(fsl *FakeSvcLoader, name string) *PQueue {
	d := getDesc()
	c := getConfig()
	d.Name = name
	q := InitPQueue(fsl, d, c)
	fsl.data[name] = q
	return q
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
	defer q.Close()
	Convey("Test push and pop messages", t, func() {
		q.Push("data1", "p1", 10000, 0, 12)
		q.Push("data2", "p2", 10000, 0, 12)
		VerifyServiceSize(q, 2)
		VerifyItems(q.Pop(10000, 0, 10, true), 2, "data1", "p1", "data2", "p2")
		VerifyServiceSize(q, 2)

		// Unlock item data1 it should become available again.
		q.UpdateLockById("data1", 0)
		q.checkTimeouts(Uts() + 110)
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
	defer q.Close()
	Convey("Two messages should expire, one message should still be in the queue", t, func() {
		q.Push("data1", "p1", 1000, 0, 12)
		q.Push("data2", "p2", 1000, 0, 12)
		q.Push("data3", "p3", 10000, 0, 12)
		VerifyServiceSize(q, 3)
		q.checkTimeouts(Uts() + 1300)
		VerifyServiceSize(q, 1)
	})
}

func TestUnlockById(t *testing.T) {
	q := CreateNewTestQueue()
	defer q.Close()
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
	defer q.Close()
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
	defer q.Close()
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
	defer q.Close()
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
	defer q.Close()
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
			resp, ok := q.Pop(0, 10, 10, false).(*ItemsResponse)
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
	defer q.Close()
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
		defer q.Close()
		Convey("Empty status should be default", func() {
			s, _ := q.GetCurrentStatus().(*DictResponse)
			status := s.GetDict()
			So(status[PQ_STATUS_MAX_QUEUE_SIZE], ShouldEqual, 100001)
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
			So(q.GetTypeName(), ShouldEqual, STYPE_PRIORITY_QUEUE)
		})
		Convey("Status for several messages in flight", func() {
			q.Push("d1", "p", 10000, 0, 9)
			q.Push("d2", "p", 10000, 0, 9)
			q.Push("d3", "p", 10000, 0, 9)

			VerifySingleItem(q.Pop(100000, 0, 1, true), "d1", "p")
			VerifyServiceSize(q, 3)

			s, _ := q.GetCurrentStatus().(*DictResponse)
			status := s.GetDict()
			So(status[PQ_STATUS_MAX_QUEUE_SIZE], ShouldEqual, 100001)
			So(status[PQ_STATUS_MSG_TTL], ShouldEqual, 100000)
			So(status[PQ_STATUS_DELIVERY_DELAY], ShouldEqual, 1)
			So(status[PQ_STATUS_POP_LOCK_TIMEOUT], ShouldEqual, 10000)
			So(status[PQ_STATUS_POP_COUNT_LIMIT], ShouldEqual, 4)
			So(status[PQ_STATUS_CREATE_TS], ShouldBeLessThanOrEqualTo, Uts())
			So(status[PQ_STATUS_LAST_PUSH_TS], ShouldBeLessThanOrEqualTo, Uts())
			So(status[PQ_STATUS_LAST_POP_TS], ShouldBeLessThanOrEqualTo, Uts())
			So(status[PQ_STATUS_TOTAL_MSGS], ShouldEqual, 3)
			So(status[PQ_STATUS_IN_FLIGHT_MSG], ShouldEqual, 1)
			So(status[PQ_STATUS_AVAILABLE_MSGS], ShouldEqual, 2)
		})

	})
}

func TestSetParams(t *testing.T) {
	Convey("Parameters should be set", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		VerifyOkResponse(q.SetParams(10000, 256000, 20000, 30000, 40000, 50000, ""))

		s, _ := q.GetCurrentStatus().(*DictResponse)
		status := s.GetDict()
		So(status[PQ_STATUS_MSG_TTL], ShouldEqual, 10000)
		So(status[PQ_STATUS_MAX_MSG_SIZE], ShouldEqual, 256000)
		So(status[PQ_STATUS_MAX_QUEUE_SIZE], ShouldEqual, 20000)
		So(status[PQ_STATUS_DELIVERY_DELAY], ShouldEqual, 30000)
		So(status[PQ_STATUS_POP_COUNT_LIMIT], ShouldEqual, 40000)
		So(status[PQ_STATUS_POP_LOCK_TIMEOUT], ShouldEqual, 50000)
		So(status[PQ_STATUS_FAIL_QUEUE], ShouldEqual, "")
	})
}

func TestGetMessageInfo(t *testing.T) {
	Convey("Pushed message should return parameters", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 1000, 9)
		q.Push("d2", "p", 10000, 0, 11)

		So(q.GetMessageInfo("d3"), ShouldResemble, ERR_MSG_NOT_FOUND)

		m1, _ := q.GetMessageInfo("d1").(*DictResponse)
		m2, _ := q.GetMessageInfo("d2").(*DictResponse)

		msgInfo1 := m1.GetDict()
		msgInfo2 := m2.GetDict()

		So(msgInfo1[MSG_INFO_ID], ShouldEqual, "d1")
		So(msgInfo1[MSG_INFO_LOCKED], ShouldEqual, true)
		So(msgInfo1[MSG_INFO_UNLOCK_TS], ShouldBeGreaterThan, Uts())
		So(msgInfo1[MSG_INFO_POP_COUNT], ShouldEqual, 0)
		So(msgInfo1[MSG_INFO_PRIORITY], ShouldEqual, 9)
		So(msgInfo1[MSG_INFO_EXPIRE_TS], ShouldBeGreaterThan, Uts())

		So(msgInfo2[MSG_INFO_ID], ShouldEqual, "d2")
		So(msgInfo2[MSG_INFO_LOCKED], ShouldEqual, false)
		So(msgInfo2[MSG_INFO_UNLOCK_TS], ShouldBeLessThanOrEqualTo, Uts())
		So(msgInfo2[MSG_INFO_POP_COUNT], ShouldEqual, 0)
		So(msgInfo2[MSG_INFO_PRIORITY], ShouldEqual, 11)
		So(msgInfo2[MSG_INFO_EXPIRE_TS], ShouldBeGreaterThan, Uts())

	})
}

func TestUnlockErrors(t *testing.T) {
	Convey("Attempts to unlock not locked and not existing messages should result in error", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 0, 11)
		So(q.UnlockMessageById("d1"), ShouldResemble, ERR_MSG_NOT_LOCKED)
		So(q.UnlockMessageById("d2"), ShouldResemble, ERR_MSG_NOT_FOUND)
	})
}

func TestDeleteMessageErrors(t *testing.T) {
	Convey("Attempts to delete locked and not existing message should result in error", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 100, 11)
		So(q.DeleteById("d1"), ShouldResemble, ERR_MSG_IS_LOCKED)
		So(q.DeleteById("d2"), ShouldResemble, ERR_MSG_NOT_FOUND)
	})
}

func TestPushError(t *testing.T) {
	q := CreateNewTestQueue()
	defer q.Close()
	q.Push("d1", "p", 10000, 100, 11)
	Convey("Push should result in errors", t, func() {
		So(q.Push("d1", "p", 10000, 100, 1), ShouldResemble, ERR_ITEM_ALREADY_EXISTS)
	})
}

func TestExpiration(t *testing.T) {
	Convey("One item should expire", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 0, 11)
		r, _ := q.TimeoutItems(Uts() + 100000).(*IntResponse)
		So(r.Value, ShouldEqual, 1)
		VerifyServiceSize(q, 0)
	})
}

func TestReleaseInFlight(t *testing.T) {
	Convey("One item should expire", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 100, 11)
		r, _ := q.ReleaseInFlight(Uts() + 1000).(*IntResponse)
		So(r.Value, ShouldEqual, 1)
		VerifySingleItem(q.Pop(0, 0, 1, false), "d1", "p")
	})
}

func TestPopCountExpiration(t *testing.T) {
	Convey("Item should disappear on fifth attempt to pop it", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
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
		VerifyItemsRespSize(q.Pop(0, 0, 1, true), 0)

		VerifyServiceSize(q, 0)
	})
}

func TestSize(t *testing.T) {
	Convey("Size of different structures should be valid", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
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

func TestUnlockByReceipt(t *testing.T) {
	Convey("Unlock by receipt should have correct behavior", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 0, 11)
		resp := q.Pop(100000, 0, 2, true)
		VerifyServiceSize(q, 1)
		VerifyItemsRespSize(resp, 1)

		rcpt := resp.(*ItemsResponse).GetItems()[0].(*MsgResponseItem).GetReceipt()
		So(len(rcpt), ShouldBeGreaterThan, 2)
		VerifyOkResponse(q.UnlockByReceipt(rcpt))
		VerifyServiceSize(q, 1)

		So(q.UnlockByReceipt(rcpt), ShouldEqual, ERR_RECEIPT_EXPIRED)
		q.Pop(100000, 0, 2, true)
		So(q.UnlockByReceipt(rcpt), ShouldEqual, ERR_RECEIPT_EXPIRED)
	})
}

func TestDeleteByReceipt(t *testing.T) {
	Convey("Delete by receipt should have correct behavior", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 0, 11)
		resp := q.Pop(100000, 0, 2, true)
		VerifyServiceSize(q, 1)
		VerifyItemsRespSize(resp, 1)

		rcpt := resp.(*ItemsResponse).GetItems()[0].(*MsgResponseItem).GetReceipt()
		So(len(rcpt), ShouldBeGreaterThan, 2)
		VerifyOkResponse(q.DeleteByReceipt(rcpt))
		VerifyServiceSize(q, 0)

		So(q.UnlockByReceipt(rcpt), ShouldEqual, ERR_RECEIPT_EXPIRED)
	})
}

func TestUpdateLockByReceipt(t *testing.T) {
	Convey("Delete by receipt should have correct behavior", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 0, 11)
		resp := q.Pop(100000, 0, 2, true)
		VerifyServiceSize(q, 1)
		VerifyItemsRespSize(resp, 1)

		rcpt := resp.(*ItemsResponse).GetItems()[0].(*MsgResponseItem).GetReceipt()
		So(len(rcpt), ShouldBeGreaterThan, 2)
		VerifyOkResponse(q.UpdateLockByRcpt(rcpt, 10000))
		VerifyServiceSize(q, 1)
	})
}

func TestSizeLimit(t *testing.T) {
	Convey("Fourth element should fail with size limit error", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.SetParams(10000, 256000, 3, 10000, 0, 50000, "")
		VerifyOkResponse(q.Push("1", "p", 10000, 0, 11))
		VerifyOkResponse(q.Push("2", "p", 10000, 0, 11))
		VerifyOkResponse(q.Push("3", "p", 10000, 0, 11))
		So(q.Push("4", "p", 10000, 0, 11), ShouldResemble, ERR_SIZE_EXCEEDED)
		VerifyServiceSize(q, 3)
	})
}

func TestMessagesMovedToAnotherQueue(t *testing.T) {
	Convey("Elements should move from one queue to the other when number of pop attempts exceeded", t, func() {

		log.InitLogging()
		log.SetLevel(1)
		db.SetDatabase(testutils.NewInMemDBService())

		fsl := NewFakeSvcLoader()
		q1 := CreateTestQueueWithName(fsl, "q1")
		failQueue := CreateTestQueueWithName(fsl, "fq")
		q1.SetParams(100000, 256000, 10000, 0, 2, 1000, "fq")

		q1.StartUpdate()
		failQueue.StartUpdate()

		defer q1.Close()
		defer failQueue.Close()

		Convey("Two elements should be moved to another queue", func() {
			VerifyOkResponse(q1.Push("d1", "p", 10000, 0, 11))
			VerifyOkResponse(q1.Push("d2", "p", 10000, 0, 11))
			VerifyServiceSize(q1, 2)

			VerifyItemsRespSize(q1.Pop(100, 0, 10, true), 2)
			q1.checkTimeouts(Uts() + 10000)
			VerifyServiceSize(q1, 2)

			VerifyItemsRespSize(q1.Pop(100, 0, 10, true), 2)
			q1.checkTimeouts(Uts() + 10000)
			VerifyServiceSize(q1, 0)

			// Need to wait while message transferring is happening.
			for i := 0; i < 10000; i++ {
				time.Sleep(time.Microsecond * 1)
				if failQueue.GetSize() == 2 {
					break
				}
			}
			VerifyServiceSize(failQueue, 2)
		})

	})
}
