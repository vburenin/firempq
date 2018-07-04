package pqueue

import (
	"os"
	"testing"
	"time"

	"strconv"

	"github.com/stretchr/testify/assert"
	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/db/linear"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/queue_info"
	"github.com/vburenin/firempq/utils"
)

func getConfig() *conf.PQConfig {
	return &conf.PQConfig{
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

func getDesc() *queue_info.ServiceDescription {
	return &queue_info.ServiceDescription{
		ExportId:  10,
		SType:     "PQueue",
		Name:      "name",
		CreateTs:  123,
		Disabled:  false,
		ToDelete:  false,
		ServiceId: "1",
	}
}

type QueueGetterForTest struct {
	q map[string]*PQueue
}

func NewQueueGetterForTest() *QueueGetterForTest {
	return &QueueGetterForTest{q: make(map[string]*PQueue)}
}
func (qgt *QueueGetterForTest) Add(q *PQueue)                   { qgt.q[q.desc.Name] = q }
func (qgt *QueueGetterForTest) queueGetter(name string) *PQueue { return qgt.q[name] }

func CreateTestQueueWithName(db apis.DataStorage, qg *QueueGetterForTest, name string) *PQueue {
	d := getDesc()
	c := getConfig()
	d.Name = name
	q := NewPQueue(qg.queueGetter, db, d, c)
	qg.Add(q)
	return q
}

func WipeTestQueueData() {
	err := os.RemoveAll("testdata/tempdb")
	if err != nil {
		panic("failed to remove db")
	}
}

const PostOptionWipe = "wipe"
const PreOptionWipe = "prewipe"

func CreateSingleQueue(options ...string) (*PQueue, func()) {
	log.InitLogging()
	conf.UseDefaultsOnly()

	for _, v := range options {
		if v == PreOptionWipe {
			WipeTestQueueData()
		}
	}

	db, err := linear.NewFlatStorage("testdata/tempdb", 1024*1024, 1024*1024)
	if err != nil {
		log.Fatal("could not create db: %s", err)
	}
	qg := NewQueueGetterForTest()
	q := NewPQueue(qg.queueGetter, db, getDesc(), getConfig())
	f := func() {
		q.Close()
		db.Close()
		for _, v := range options {
			if v == PostOptionWipe {
				WipeTestQueueData()
			}
		}
	}
	return q, f
}

func cmp(t *testing.T, a, b string) {
	if a != b {
		t.Error("Unexpected value '" + a + "'. Expecting: '" + b + "'")
	}
}

func TestPushPopAndTimeUnlockItems(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	//defer WipeTestQueueData()

	q.Push("data1", "p1", 10000, 0)
	q.Push("data2", "p2", 10000, 0)
	a.Equal(uint64(2), q.TotalMessages())

	items := q.Pop(10000, 0, 10, true)
	VerifyItems(a, items, 2, "data1", "p1", "data2", "p2")
	a.Equal(uint64(2), q.TotalMessages())

	// Unlock item data1 it should become available again.
	q.UpdateLockById("data1", 0)
	a.Equal(int64(1), q.checkTimeouts(utils.Uts()+110))
	item := q.Pop(10000, 0, 1, true)
	VerifySingleItem(a, item, "data1", "p1")
	a.Equal(uint64(2), q.TotalMessages())

	VerifyOkResponse(a, q.DeleteLockedById("data1"))

	a.Equal(uint64(1), q.TotalMessages())
	q.DeleteLockedById("data2")
	a.Equal(uint64(0), q.TotalMessages())

}

func TestAutoExpiration(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	q.Push("data1", "p1", 1000, 0)
	q.Push("data2", "p2", 1000, 0)
	q.Push("data3", "p3", 10000, 0)
	a.Equal(uint64(3), q.TotalMessages())
	q.checkTimeouts(utils.Uts() + 1300)
	a.Equal(uint64(1), q.TotalMessages())

}

// Locked message should become visible again after it gets unlocked
func TestUnlockById(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	q.Push("data1", "p1", 1000, 0)
	q.Push("data2", "p2", 1000, 0)
	VerifyItems(a, q.Pop(10000, 0, 10, true), 2, "data1", "p1", "data2", "p2")
	VerifyItems(a, q.Pop(10000, 0, 10, true), 0)
	VerifyOkResponse(a, q.UnlockMessageById("data2"))
	VerifyItems(a, q.Pop(10000, 0, 10, true), 1, "data2", "p2")

}

// Delete not locked message
func TestDeleteById(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	q.Push("data1", "p1", 1000, 0)
	a.Equal(uint64(1), q.TotalMessages())
	VerifyOkResponse(a, q.DeleteById("data1"))
	a.Equal(uint64(0), q.TotalMessages())
	items := q.Pop(10000, 0, 10, true)
	VerifyItems(a, items, 0)
}

// Locked message should be removed
func TestDeleteLockedById(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	q.Push("data1", "p1", 10000, 0)
	q.Push("data2", "p2", 10000, 0)
	items := q.Pop(10000, 0, 10, true)
	VerifyItems(a, items, 2, "data1", "p1", "data2", "p2")
	VerifyOkResponse(a, q.DeleteLockedById("data1"))
	VerifyOkResponse(a, q.DeleteLockedById("data2"))
	a.Equal(uint64(0), q.TotalMessages())
}

// Messages should be delivered after 0.1 seconds
// TODO(vburenin): potentially not reliable test.
func TestPopWaitBatch(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	go func() {
		time.Sleep(time.Second / 10)
		q.Push("d1", "1", 10000, 0)
		q.Push("d2", "2", 10000, 0)
		q.Push("d3", "3", 10000, 0)
	}()
	items := q.Pop(10000, 1000, 10, true)
	VerifyItems(a, items, 3, "d1", "1", "d2", "2", "d3", "3")
}

// Messages should be delivered after 0.1 seconds
func TestPopWaitTimeout(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	q.Push("d1", "1", 100000, 1000)
	q.Push("d2", "2", 100000, 1000)
	q.Push("d3", "3", 100000, 1000)

	items := q.Pop(0, 10, 10, true)
	VerifyItems(a, items, 0)
	a.Equal(uint64(3), q.TotalMessages())
	q.checkTimeouts(utils.Uts() + 10001)
	items = q.Pop(0, 0, 10, true)
	VerifyItems(a, items, 3, "d1", "1", "d2", "2", "d3", "3")
}

// Message delivery delay should be delayed at least for 0.1 seconds
func TestDeliveryDelay(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()
	q.StartUpdate()

	q.Push("data1", "p1", 10000, 120)
	startTs := time.Now()
	item := q.Pop(10000, 1000, 10, true)
	VerifyItems(a, item, 1, "data1", "p1")
	finishTs := time.Now()

	a.True(finishTs.Sub(startTs) >= time.Second/10)
}

// 10k messages should be pushed and received. Messages are not locked.
func TestPushLotsOfMessages(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	q.StartUpdate()

	totalMsg := 10000

	for i := 0; i < totalMsg; i++ {
		q.Push("id"+strconv.Itoa(i), " ", 100000, 0)
	}
	work := true
	go func() {
		time.Sleep(time.Second * 5)
		work = false
	}()
	counter := 0
	for counter < totalMsg && work {
		resp, ok := q.Pop(0, 10, 10, false).(*resp.MessagesResponse)
		if !ok {
			break
		}
		counter += len(resp.GetItems())
	}
	a.Equal(totalMsg, counter)
	a.Equal(uint64(0), q.TotalMessages())
}

/*
// Push some messages and load them
func TestMessageLoad(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	q.StartUpdate()

	q.Push("d1", "p", 100000, 0)
	q.Push("d2", "p", 100000, 0)
	q.Push("d3", "p", 100000, 0)
	q.Push("d4", "p", 100000, 0)
	q.Push("d5", "p", 100000, 0)
	q.Push("d6", "p", 100000, 0)
	q.Push("d0", "p", 100000, 0)

	// This messages should be removed as expired during reload.
	VerifyOkResponse(a, q.Push("dd", "dd", 0, 0))
	a.Equal(uint64(8), q.TotalMessages())
	// Queue size should be the same since message is just locked.
	VerifySingleItem(a, q.Pop(1000, 0, 1, true), "d0", "p")
	a.Equal(uint64(8), q.TotalMessages())

	q.Close()

	So(q.IsClosed(), ShouldBeTrue)

	q := CreateTestQueue()
	// 7 messages because one of them has expired during reload.
	VerifyServiceSize(q, 7)
	VerifySingleItem(q.Pop(0, 0, 1, false), "d1", "p")
	VerifySingleItem(q.Pop(0, 0, 1, false), "d2", "p")
	VerifySingleItem(q.Pop(0, 0, 1, false), "d3", "p")
	VerifySingleItem(q.Pop(0, 0, 1, false), "d4", "p")
	VerifySingleItem(q.Pop(0, 0, 1, false), "d5", "p")
	VerifySingleItem(q.Pop(0, 0, 1, false), "d6", "p")
	VerifyServiceSize(q, 1)
	VerifyOkResponse(q.DeleteLockedById("d0"))
	VerifyServiceSize(q, 0)
	q.Clear()

}


func TestStatus(t *testing.T) {
	Convey("Queue status should be correct", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		Convey("Empty status should be default", func() {
			s, _ := q.GetCurrentStatus().(*resp.DictResponse)
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

			So(q.Info().ID, ShouldEqual, "1")
		})
		Convey("Status for several messages in flight", func() {
			q.Push("d1", "p", 10000, 0)
			q.Push("d2", "p", 10000, 0)
			q.Push("d3", "p", 10000, 0)

			VerifySingleItem(q.Pop(100000, 0, 1, true), "d1", "p")
			VerifyServiceSize(q, 3)

			s, _ := q.GetCurrentStatus().(*resp.DictResponse)
			status := s.GetDict()
			So(status[PQ_STATUS_MAX_QUEUE_SIZE], ShouldEqual, 100001)
			So(status[PQ_STATUS_MSG_TTL], ShouldEqual, 100000)
			So(status[PQ_STATUS_DELIVERY_DELAY], ShouldEqual, 1)
			So(status[PQ_STATUS_POP_LOCK_TIMEOUT], ShouldEqual, 10000)
			So(status[PQ_STATUS_POP_COUNT_LIMIT], ShouldEqual, 4)
			So(status[PQ_STATUS_CREATE_TS], ShouldBeLessThanOrEqualTo, utils.Uts())
			So(status[PQ_STATUS_LAST_PUSH_TS], ShouldBeLessThanOrEqualTo, utils.Uts())
			So(status[PQ_STATUS_LAST_POP_TS], ShouldBeLessThanOrEqualTo, utils.Uts())
			So(status[PQ_STATUS_TOTAL_MSGS], ShouldEqual, 3)
			So(status[PQ_STATUS_IN_FLIGHT_MSG], ShouldEqual, 1)
			So(status[PQ_STATUS_AVAILABLE_MSGS], ShouldEqual, 2)
		})

	})
}

func int64Ptr(v int64) *int64 {
	return &v
}

func TestSetParams(t *testing.T) {
	Convey("Parameters should be set", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()

		p := &PQueueParams{
			MsgTTL:         int64Ptr(10000),
			MaxMsgSize:     int64Ptr(256000),
			MaxMsgsInQueue: int64Ptr(20000),
			DeliveryDelay:  int64Ptr(30000),
			PopCountLimit:  int64Ptr(40000),
			PopLockTimeout: int64Ptr(50000),
			FailQueue:      "",
		}
		VerifyOkResponse(q.SetParams(p))

		s, _ := q.GetCurrentStatus().(*resp.DictResponse)
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
		q.Push("d1", "p", 10000, 1000)
		q.Push("d2", "p", 10000, 0)

		So(q.GetMessageInfo("d3"), ShouldResemble, mpqerr.ERR_MSG_NOT_FOUND)

		m1, _ := q.GetMessageInfo("d1").(*resp.DictResponse)
		m2, _ := q.GetMessageInfo("d2").(*resp.DictResponse)

		msgInfo1 := m1.GetDict()
		msgInfo2 := m2.GetDict()

		So(msgInfo1[MSG_INFO_ID], ShouldEqual, "d1")
		So(msgInfo1[MSG_INFO_LOCKED], ShouldEqual, true)
		So(msgInfo1[MSG_INFO_UNLOCK_TS], ShouldBeGreaterThan, utils.Uts())
		So(msgInfo1[MSG_INFO_POP_COUNT], ShouldEqual, 0)
		So(msgInfo1[MSG_INFO_PRIORITY], ShouldEqual, 9)
		So(msgInfo1[MSG_INFO_EXPIRE_TS], ShouldBeGreaterThan, utils.Uts())

		So(msgInfo2[MSG_INFO_ID], ShouldEqual, "d2")
		So(msgInfo2[MSG_INFO_LOCKED], ShouldEqual, false)
		So(msgInfo2[MSG_INFO_UNLOCK_TS], ShouldBeLessThanOrEqualTo, utils.Uts())
		So(msgInfo2[MSG_INFO_POP_COUNT], ShouldEqual, 0)
		So(msgInfo2[MSG_INFO_PRIORITY], ShouldEqual, 11)
		So(msgInfo2[MSG_INFO_EXPIRE_TS], ShouldBeGreaterThan, utils.Uts())

	})
}

func TestUnlockErrors(t *testing.T) {
	Convey("Attempts to unlock not locked and not existing messages should result in error", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 0)
		So(q.UnlockMessageById("d1"), ShouldResemble, mpqerr.ERR_MSG_NOT_LOCKED)
		So(q.UnlockMessageById("d2"), ShouldResemble, mpqerr.ERR_MSG_NOT_FOUND)
	})
}

func TestDeleteMessageErrors(t *testing.T) {
	Convey("Attempts to delete locked and not existing message should result in error", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 100)
		So(q.DeleteById("d1"), ShouldResemble, mpqerr.ERR_MSG_IS_LOCKED)
		So(q.DeleteById("d2"), ShouldResemble, mpqerr.ERR_MSG_NOT_FOUND)
	})
}

func TestPushError(t *testing.T) {
	q := CreateNewTestQueue()
	defer q.Close()
	q.Push("d1", "p", 10000, 100)
	Convey("Push should result in errors", t, func() {
		So(q.Push("d1", "p", 10000, 100), ShouldResemble, mpqerr.ERR_ITEM_ALREADY_EXISTS)
	})
}

func TestExpiration(t *testing.T) {
	Convey("One item should expire", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 0)
		r, _ := q.TimeoutItems(utils.Uts() + 100000).(*resp.IntResponse)
		So(r.Value, ShouldEqual, 1)
		VerifyServiceSize(q, 0)
	})
}

func TestReleaseInFlight(t *testing.T) {
	Convey("One item should expire", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 100)
		r, _ := q.ReleaseInFlight(utils.Uts() + 1000).(*resp.IntResponse)
		So(r.Value, ShouldEqual, 1)
		VerifySingleItem(q.Pop(0, 0, 1, false), "d1", "p")
	})
}

func TestPopCountExpiration(t *testing.T) {
	Convey("Item should disappear on fifth attempt to pop it", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 0)
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
		q.Push("d1", "p", 10000, 0)
		q.Push("d2", "p", 10000, 0)
		q.Push("d3", "p", 10000, 0)
		q.Push("d4", "p", 10000, 0)
		q.Push("d5", "p", 10000, 0)
		q.Pop(0, 0, 2, true)

		VerifyServiceSize(q, 5)
		So(q.availMsgs.Size(), ShouldEqual, 3)
		So(q.lockedMsgCnt, ShouldEqual, 2)
		So(len(q.id2msg), ShouldEqual, 5)
	})
}

func TestUnlockByReceipt(t *testing.T) {
	Convey("Unlock by receipt should have correct behavior", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 0)
		r := q.Pop(100000, 0, 2, true)
		VerifyServiceSize(q, 1)
		VerifyItemsRespSize(r, 1)

		rcpt := r.(*resp.MessagesResponse).GetItems()[0].(*MsgResponseItem).Receipt()
		So(len(rcpt), ShouldBeGreaterThan, 2)
		VerifyOkResponse(q.UnlockByReceipt(rcpt))
		VerifyServiceSize(q, 1)

		// Unlocking message using the same receipt should succeed.
		So(q.UnlockByReceipt(rcpt), ShouldEqual, resp.OK)
		q.Pop(100000, 0, 2, true)
		So(q.UnlockByReceipt(rcpt), ShouldEqual, mpqerr.ERR_RECEIPT_EXPIRED)
	})
}

func TestDeleteByReceipt(t *testing.T) {
	Convey("Delete by receipt should have correct behavior", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 0)
		r := q.Pop(100000, 0, 2, true)
		VerifyServiceSize(q, 1)
		VerifyItemsRespSize(r, 1)

		rcpt := r.(*resp.MessagesResponse).GetItems()[0].(*MsgResponseItem).Receipt()
		So(len(rcpt), ShouldBeGreaterThan, 2)
		VerifyOkResponse(q.DeleteByReceipt(rcpt))
		VerifyServiceSize(q, 0)

		So(q.UnlockByReceipt(rcpt), ShouldEqual, mpqerr.ERR_RECEIPT_EXPIRED)
	})
}

func TestUpdateLockByReceipt(t *testing.T) {
	Convey("Delete by receipt should have correct behavior", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		q.Push("d1", "p", 10000, 0)
		r := q.Pop(100000, 0, 2, true)
		VerifyServiceSize(q, 1)
		VerifyItemsRespSize(r, 1)

		rcpt := r.(*resp.MessagesResponse).GetItems()[0].(*MsgResponseItem).Receipt()
		So(len(rcpt), ShouldBeGreaterThan, 2)
		VerifyOkResponse(q.UpdateLockByRcpt(rcpt, 10000))
		VerifyServiceSize(q, 1)
	})
}

func TestSizeLimit(t *testing.T) {
	Convey("Fourth element should fail with size limit error", t, func() {
		q := CreateNewTestQueue()
		defer q.Close()
		p := &PQueueParams{
			MsgTTL:         int64Ptr(10000),
			MaxMsgSize:     int64Ptr(256000),
			MaxMsgsInQueue: int64Ptr(3),
			DeliveryDelay:  int64Ptr(10000),
			PopCountLimit:  int64Ptr(0),
			PopLockTimeout: int64Ptr(50000),
			FailQueue:      "",
		}
		q.SetParams(p)
		VerifyOkResponse(q.Push("1", "p", 10000, 0))
		VerifyOkResponse(q.Push("2", "p", 10000, 0))
		VerifyOkResponse(q.Push("3", "p", 10000, 0))
		So(q.Push("4", "p", 10000, 0), ShouldResemble, mpqerr.ERR_SIZE_EXCEEDED)
		VerifyServiceSize(q, 3)
	})
}

func TestMessagesMovedToAnotherQueue(t *testing.T) {
	Convey("Elements should move from one queue to the other when number of pop attempts exceeded", t, func() {

		log.InitLogging()
		log.SetLevel(1)
		db.SetDatabase(NewInMemDBService())

		fsl := NewFakeSvcLoader()
		q1 := CreateTestQueueWithName(fsl, "q1")
		failQueue := CreateTestQueueWithName(fsl, "fq")
		p := &PQueueParams{
			MsgTTL:         int64Ptr(10000),
			MaxMsgSize:     int64Ptr(256000),
			MaxMsgsInQueue: int64Ptr(100000),
			DeliveryDelay:  int64Ptr(0),
			PopCountLimit:  int64Ptr(2),
			PopLockTimeout: int64Ptr(1000),
			FailQueue:      "fq",
		}
		q1.SetParams(p)

		q1.StartUpdate()
		failQueue.StartUpdate()

		defer q1.Close()
		defer failQueue.Close()

		Convey("Two elements should be moved to another queue", func() {
			VerifyOkResponse(q1.Push("d1", "p", 10000, 0))
			VerifyOkResponse(q1.Push("d2", "p", 10000, 0))
			VerifyServiceSize(q1, 2)

			VerifyItemsRespSize(q1.Pop(100, 0, 10, true), 2)
			q1.checkTimeouts(utils.Uts() + 10000)
			VerifyServiceSize(q1, 2)

			VerifyItemsRespSize(q1.Pop(100, 0, 10, true), 2)
			q1.checkTimeouts(utils.Uts() + 10000)
			VerifyServiceSize(q1, 0)

			// Need to wait while message transferring is happening.
			for i := 0; i < 10000; i++ {
				time.Sleep(time.Microsecond * 1)
				if failQueue.Info().Size == 2 {
					break
				}
			}
			VerifyServiceSize(failQueue, 2)
		})

	})
}
*/

func VerifyItemsRespSize(a *assert.Assertions, r apis.IResponse, size int) ([]apis.IResponseItem, bool) {
	ir, ok := r.(*resp.MessagesResponse)
	a.True(ok)
	if ok {
		items := ir.GetItems()
		a.Equal(size, len(items))
		return items, len(items) == size
	}
	return nil, false
}

func VerifySingleItem(a *assert.Assertions, r apis.IResponse, itemId, payload string) bool {
	if items, ok := VerifyItemsRespSize(a, r, 1); ok {
		return a.Equal(itemId, items[0].ID()) && a.Equal(payload, string(items[0].Payload()))
	}
	return false
}

func VerifyOkResponse(a *assert.Assertions, r apis.IResponse) bool {
	return a.Equal(resp.OK, r)
}

func VerifyItems(a *assert.Assertions, r apis.IResponse, size int, itemSpecs ...string) bool {
	a.Equal(size*2, len(itemSpecs))
	items, ok := VerifyItemsRespSize(a, r, size)
	if size*2 == len(itemSpecs) && ok {
		for i := 0; i < len(itemSpecs); i += 2 {
			itemPos := i / 2
			itemId := itemSpecs[i]
			itemPayload := itemSpecs[i+1]
			a.Equal(itemId, items[itemPos].ID())
			a.Equal(itemPayload, string(items[itemPos].Payload()))
		}
		return true
	}
	return false
}
