package pqueue

import (
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/db/linear"
	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/pmsg"
	"github.com/vburenin/firempq/qconf"
	"github.com/vburenin/firempq/utils"
	"go.uber.org/zap"
)

func getConfig() *qconf.QueueConfig {
	return &qconf.QueueConfig{
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

func getDesc() *qconf.QueueDescription {
	return &qconf.QueueDescription{
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
		log.Fatal("could not create db", zap.Error(err))
	}
	deadMsgs := make(chan DeadMessage, 16)
	q := NewPQueue(db, getDesc(), getConfig(), deadMsgs, nil)
	f := func() {
		db.Close()
		for _, v := range options {
			if v == PostOptionWipe {
				WipeTestQueueData()
			}
		}
	}
	return q, f
}

func CreateQueueManager(a *assert.Assertions, options ...string) (*QueueManager, func()) {
	log.InitLogging()
	conf.UseDefaultsOnly()

	ctx := fctx.Background("test")

	for _, v := range options {
		if v == PreOptionWipe {
			WipeTestQueueData()
		}
	}
	conf.CFG.DatabasePath = "testdata/tempdb"

	os.MkdirAll(conf.CFG.DatabasePath, 0755)

	qm, err := NewQueueManager(ctx, conf.CFG)
	if err != nil {
		a.FailNow("queue manager not initialized", err.Error())
	}

	f := func() {
		qm.Close()
		for _, v := range options {
			if v == PostOptionWipe {
				WipeTestQueueData()
			}
		}
	}
	return qm, f
}

// Push some messages and load them
func TestMessageLoad(t *testing.T) {
	a := assert.New(t)
	qm, closer := CreateQueueManager(a, PreOptionWipe)
	ctx := fctx.Background("test")

	qm.CreateQueue(ctx, "test-msg-load", getConfig())

	q := qm.GetQueue("test-msg-load")
	if !a.NotNil(q) {
		return
	}
	PushVerifyOk(a, q, "d0", "p", 100000, 0)
	PushVerifyOk(a, q, "d1", "p", 100000, 0)
	PushVerifyOk(a, q, "d2", "p", 100000, 0)
	PushVerifyOk(a, q, "d3", "p", 100000, 0)
	PushVerifyOk(a, q, "d4", "p", 100000, 0)
	PushVerifyOk(a, q, "d5", "p", 100000, 0)
	PushVerifyOk(a, q, "d6", "p", 100000, 0)

	// This messages should be removed as expired during reload.
	PushVerifyOk(a, q, "dd", "dd", 0, 0)

	a.Equal(uint64(8), q.TotalMessages())
	// Queue size should be the same since message is just locked.
	VerifySingleItem(a, q.Pop(1000, 0, 1, true), "d0", "p")
	a.Equal(uint64(8), q.TotalMessages())

	closer()

	qm, closer = CreateQueueManager(a, PostOptionWipe)
	defer closer()
	q = qm.GetQueue("test-msg-load")
	if !a.NotNil(q) {
		return
	}

	// 7 messages because one of them has expired during reload.
	a.Equal(uint64(7), q.TotalMessages())

	VerifySingleItem(a, q.Pop(0, 0, 1, false), "d1", "p")
	VerifySingleItem(a, q.Pop(0, 0, 1, false), "d2", "p")
	VerifySingleItem(a, q.Pop(0, 0, 1, false), "d3", "p")
	VerifySingleItem(a, q.Pop(0, 0, 1, false), "d4", "p")
	VerifySingleItem(a, q.Pop(0, 0, 1, false), "d5", "p")
	VerifySingleItem(a, q.Pop(0, 0, 1, false), "d6", "p")

	a.Equal(uint64(1), q.TotalMessages())
	VerifyOkResponse(a, q.DeleteLockedById("d0"))
	a.Equal(uint64(0), q.TotalMessages())
	ctx.Debug("datatadata", zap.String("asdasd", "asdasdas"), zap.Uint8("asdasd", 12))
}

func TestPushPopAndTimeUnlockItems(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	//defer WipeTestQueueData()

	PushVerifyOk(a, q, "data1", "p1", 10000, 0)
	PushVerifyOk(a, q, "data2", "p2", 10000, 0)
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

	PushVerifyOk(a, q, "data1", "p1", 1000, 0)
	PushVerifyOk(a, q, "data2", "p2", 1000, 0)
	PushVerifyOk(a, q, "data3", "p3", 10000, 0)
	a.Equal(uint64(3), q.TotalMessages())
	q.checkTimeouts(utils.Uts() + 1300)
	a.Equal(uint64(1), q.TotalMessages())

}

// Locked message should become visible again after it gets unlocked
func TestUnlockById(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	PushVerifyOk(a, q, "data1", "p1", 1000, 0)
	PushVerifyOk(a, q, "data2", "p2", 1000, 0)
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

	PushVerifyOk(a, q, "data1", "p1", 1000, 0)
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

	PushVerifyOk(a, q, "data1", "p1", 10000, 0)
	PushVerifyOk(a, q, "data2", "p2", 10000, 0)
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
		PushVerifyOk(a, q, "d1", "1", 10000, 0)
		PushVerifyOk(a, q, "d2", "2", 10000, 0)
		PushVerifyOk(a, q, "d3", "3", 10000, 0)
	}()
	items := q.Pop(10000, 1000, 10, true)
	VerifyItems(a, items, 3, "d1", "1", "d2", "2", "d3", "3")
}

// Messages should be delivered after 0.1 seconds
func TestPopWaitTimeout(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	PushVerifyOk(a, q, "d1", "1", 100000, 1000)
	PushVerifyOk(a, q, "d2", "2", 100000, 1000)
	PushVerifyOk(a, q, "d3", "3", 100000, 1000)

	items := q.Pop(0, 10, 10, true)
	VerifyItems(a, items, 0)
	a.Equal(uint64(3), q.TotalMessages())
	q.checkTimeouts(utils.Uts() + 10001)
	items = q.Pop(0, 0, 10, true)
	VerifyItems(a, items, 3, "d1", "1", "d2", "2", "d3", "3")
}

func updateCaller(q *PQueue) func() {
	c := make(chan struct{})
	var block sync.WaitGroup
	block.Add(1)
	go func() {
		defer block.Done()
		for {
			q.Update()
			time.Sleep(10 * time.Millisecond)
			select {
			case <-c:
				return
			default:

			}
		}
	}()
	return func() {
		close(c)
		block.Wait()
	}
}

// Message delivery delay should be delayed at least for 0.1 seconds
func TestDeliveryDelay(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	updater := updateCaller(q)
	defer updater()

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
		counter += len(resp.Messages)
	}
	a.Equal(totalMsg, counter)
	a.Equal(uint64(0), q.TotalMessages())
}

// Queue status should be correct
func TestStatus(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	s, _ := q.GetCurrentStatus().(*resp.DictResponse)
	status := s.GetDict()
	a.EqualValues(100001, status[StatusQueueMaxSize])
	a.EqualValues(100000, status[StatusQueueMsgTTL])
	a.EqualValues(1, status[StatusQueueDeliveryDelay])
	a.EqualValues(10000, status[StatusQueuePopLockTimeout])
	a.EqualValues(4, status[StatusQueuePopCountLimit])
	a.EqualValues(123, status[StatusQueueCreateTs])
	a.EqualValues(12, status[StatusQueueLastPushTs])
	a.EqualValues(13, status[StatusQueueLastPopTs])
	a.EqualValues(0, status[StatusQueueTotalMsgs])
	a.EqualValues(0, status[StatusQueueInFlightMsgs])
	a.EqualValues(0, status[StatusQueueAvailableMsgs])
}

// Status for several messages in flight
func TestStatusWithMessages(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	q.Push("d1", "p", 10000, 0)
	q.Push("d2", "p", 10000, 0)
	q.Push("d3", "p", 10000, 0)

	VerifySingleItem(a, q.Pop(100000, 0, 1, true), "d1", "p")

	s, _ := q.GetCurrentStatus().(*resp.DictResponse)
	status := s.GetDict()
	a.EqualValues(100001, status[StatusQueueMaxSize])
	a.EqualValues(100000, status[StatusQueueMsgTTL])
	a.EqualValues(1, status[StatusQueueDeliveryDelay])
	a.EqualValues(10000, status[StatusQueuePopLockTimeout])
	a.EqualValues(4, status[StatusQueuePopCountLimit])
	a.EqualValues(123, status[StatusQueueCreateTs])
	a.InDelta(utils.Uts(), status[StatusQueueLastPushTs], 100)
	a.InDelta(utils.Uts(), status[StatusQueueLastPopTs], 100)

	a.EqualValues(3, status[StatusQueueTotalMsgs])
	a.EqualValues(1, status[StatusQueueInFlightMsgs])
	a.EqualValues(2, status[StatusQueueAvailableMsgs])
}

func int64Ptr(v int64) *int64 {
	return &v
}
func strPtr(v string) *string {
	return &v
}

// Queue parameters should be updated according to the passed one.
func TestSetParams(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	p := &qconf.QueueParams{
		MsgTTL:         int64Ptr(10000),
		MaxMsgSize:     int64Ptr(256000),
		MaxMsgsInQueue: int64Ptr(20000),
		DeliveryDelay:  int64Ptr(30000),
		PopCountLimit:  int64Ptr(40000),
		PopLockTimeout: int64Ptr(50000),
		FailQueue:      strPtr("some queue"),
	}

	a.Equal(mpqerr.ErrDbProblem, q.UpdateConfig(p))

	configUpdated := false
	q.configUpdater = func(config *qconf.QueueParams) (*qconf.QueueConfig, error) {
		configUpdated = true
		return q.config, nil
	}

	a.Equal(resp.OK, q.UpdateConfig(p))
	a.True(configUpdated)

}

func castToInt64(v interface{}) int64 {
	switch a := v.(type) {
	case int:
		return int64(a)
	case int64:
		return a
	case uint64:
		return int64(a)
	default:
		panic("wrong type")
	}
}

func TestGetMessageInfo(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	q.Push("d1", "p", 10000, 1000)
	q.Push("d2", "p", 10000, 0)

	a.Equal(mpqerr.ErrMsgNotFound, q.GetMessageInfo("d3"))

	m1, _ := q.GetMessageInfo("d1").(*resp.DictResponse)
	m2, _ := q.GetMessageInfo("d2").(*resp.DictResponse)

	msgInfo1 := m1.GetDict()
	msgInfo2 := m2.GetDict()

	a.EqualValues("d1", msgInfo1[MsgInfoID])
	a.EqualValues(true, msgInfo1[MsgInfoLocked])
	a.EqualValues(0, msgInfo1[MsgInfoPopCount])
	a.True(castToInt64(msgInfo1[MsgInfoExpireTs]) > utils.Uts())
	a.True(castToInt64(msgInfo1[MsgInfoUnlockTs]) > utils.Uts())

	a.EqualValues("d2", msgInfo2[MsgInfoID])
	a.EqualValues(false, msgInfo2[MsgInfoLocked])
	a.EqualValues(0, msgInfo2[MsgInfoPopCount])
	a.True(castToInt64(msgInfo2[MsgInfoExpireTs]) > utils.Uts())
	a.True(castToInt64(msgInfo2[MsgInfoUnlockTs]) == 0)

}

// Attempts to unlock not locked and not existing messages should result in error
func TestUnlockErrors(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()
	q.Push("d1", "p", 10000, 0)
	a.Equal(mpqerr.ErrMsgNotLocked, q.UnlockMessageById("d1"))
	a.Equal(mpqerr.ErrMsgNotFound, q.UnlockMessageById("d2"))

}

// Attempts to delete locked and not existing message should result in erro
func TestDeleteMessageErrors(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	q.Push("d1", "p", 10000, 100)
	a.Equal(mpqerr.ErrMsgLocked, q.DeleteById("d1"))
	a.Equal(mpqerr.ErrMsgNotFound, q.DeleteById("d2"))

}

func TestPushError(t *testing.T) {
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()
	q.Push("d1", "p", 10000, 100)

	assert.Equal(t, mpqerr.ErrMsgAlreadyExists, q.Push("d1", "p", 10000, 100))
}

// One item should expire
func TestExpiration(t *testing.T) {
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()
	q.Push("d1", "p", 10000, 0)
	r, _ := q.TimeoutItems(utils.Uts() + 100000).(*resp.IntResponse)
	assert.EqualValues(t, 1, r.Value)
	assert.EqualValues(t, 0, q.TotalMessages())
}

// One message should ne released
func TestReleaseInFlight(t *testing.T) {
	q, closer := CreateSingleQueue(PreOptionWipe, PostOptionWipe)
	defer closer()
	q.Push("d1", "p", 10000, 100)
	r, _ := q.TimeoutItems(utils.Uts() + 1000).(*resp.IntResponse)
	assert.EqualValues(t, 1, r.Value)
	assert.EqualValues(t, 1, q.TotalMessages())
}

// Message should be automatically removed
func TestPopCountExpiration(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()
	q.Push("d1", "p", 10000, 0)
	VerifySingleItem(a, q.Pop(0, 0, 1, true), "d1", "p")
	VerifyOkResponse(a, q.UnlockMessageById("d1"))

	VerifySingleItem(a, q.Pop(0, 0, 1, true), "d1", "p")
	VerifyOkResponse(a, q.UnlockMessageById("d1"))

	VerifySingleItem(a, q.Pop(0, 0, 1, true), "d1", "p")
	VerifyOkResponse(a, q.UnlockMessageById("d1"))

	VerifySingleItem(a, q.Pop(0, 0, 1, true), "d1", "p")
	VerifyOkResponse(a, q.UnlockMessageById("d1"))

	// PopAttempts end here.
	VerifyItemsRespSize(a, q.Pop(0, 0, 1, true), 0)

	a.EqualValues(0, q.TotalMessages())

}

// Check sizes of data structures holding messages.
func TestSizeOfStructuresHoldingMessages(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()
	q.Push("d1", "p", 10000, 0)
	q.Push("d2", "p", 10000, 0)
	q.Push("d3", "p", 10000, 0)
	q.Push("d4", "p", 10000, 0)
	q.Push("d5", "p", 10000, 0)
	q.Pop(0, 0, 2, true)

	a.EqualValues(5, q.TotalMessages())
	a.EqualValues(3, q.availMsgs.Size())
	a.EqualValues(2, q.lockedMsgCnt)
	a.EqualValues(5, len(q.id2msg))
}

// Unlock messages by their receipt.
func TestUnlockByReceipt(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()
	q.Push("d1", "p", 10000, 0)
	r := q.Pop(100000, 0, 2, true)
	a.EqualValues(1, q.TotalMessages())
	VerifyItemsRespSize(a, r, 1)

	rcpt := r.(*resp.MessagesResponse).Messages[0].Receipt()
	a.True(len(rcpt) > 2)

	VerifyOkResponse(a, q.UnlockByReceipt(rcpt))
	a.EqualValues(1, q.TotalMessages())

	// Unlocking message using the same receipt should succeed.
	a.Equal(resp.OK, q.UnlockByReceipt(rcpt))
	q.Pop(100000, 0, 2, true)
	a.Equal(mpqerr.ErrExpiredRcpt, q.UnlockByReceipt(rcpt))
}

// Delete message by its receipt.
func TestDeleteByReceipt(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	q.Push("d1", "p", 10000, 0)
	r := q.Pop(100000, 0, 2, true)
	a.EqualValues(1, q.TotalMessages())
	VerifyItemsRespSize(a, r, 1)

	rcpt := r.(*resp.MessagesResponse).Messages[0].Receipt()
	a.True(len(rcpt) > 2)
	VerifyOkResponse(a, q.DeleteByReceipt(rcpt))
	a.EqualValues(0, q.TotalMessages())
	a.Equal(mpqerr.ErrExpiredRcpt, q.UnlockByReceipt(rcpt))

}

// Update lock by receipt to extend message lock time.
func TestUpdateLockByReceipt(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()

	q.Push("d1", "p", 10000, 0)
	r := q.Pop(100000, 0, 2, true)
	a.EqualValues(1, q.TotalMessages())
	VerifyItemsRespSize(a, r, 1)

	rcpt := r.(*resp.MessagesResponse).Messages[0].Receipt()
	a.True(len(rcpt) > 2)
	VerifyOkResponse(a, q.UpdateLockByRcpt(rcpt, 10000))
	a.EqualValues(1, q.TotalMessages())
}

// Test if queue accepts only limited number of messages. In this case it is limited to 3 and should fail on 4.
func TestSizeLimit(t *testing.T) {
	a := assert.New(t)
	q, closer := CreateSingleQueue(PostOptionWipe)
	defer closer()
	q.config.MaxMsgsInQueue = 3
	VerifyMsgIdResponse(a, "1", q.Push("1", "p", 10000, 0))
	VerifyMsgIdResponse(a, "2", q.Push("2", "p", 10000, 0))
	VerifyMsgIdResponse(a, "3", q.Push("3", "p", 10000, 0))
	a.Equal(mpqerr.ErrSizeExceeded, q.Push("4", "p", 10000, 0))
	a.EqualValues(3, q.TotalMessages())
}

/*
func TestMessagesMovedToAnotherQueue(t *testing.T) {
	Convey("Elements should move from one queue to the other when number of pop attempts exceeded", t, func() {

		log.InitLogging()
		log.SetLevel(1)
		db.SetDatabase(NewInMemDBService())

		fsl := NewFakeSvcLoader()
		q1 := CreateTestQueueWithName(fsl, "q1")
		failQueue := CreateTestQueueWithName(fsl, "fq")
		p := &QueueParams{
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

func VerifyItemsRespSize(a *assert.Assertions, r apis.IResponse, size int) ([]*pmsg.FullMessage, bool) {
	ir, ok := r.(*resp.MessagesResponse)
	a.True(ok)
	if ok {
		items := ir.Messages
		a.Equal(size, len(items))
		return items, len(items) == size
	}
	return nil, false
}

func VerifySingleItem(a *assert.Assertions, r apis.IResponse, itemId, payload string) bool {
	if items, ok := VerifyItemsRespSize(a, r, 1); ok {
		return a.Equal(itemId, items[0].StrId) && a.Equal(payload, string(items[0].Payload))
	}
	return false
}

func VerifyOkResponse(a *assert.Assertions, r apis.IResponse) bool {
	return a.Equal(resp.OK, r)
}

func PushVerifyOk(a *assert.Assertions, q *PQueue, msgid, payload string, msgTtl, delay int64) {
	ir := q.Push(msgid, payload, msgTtl, delay)
	r, ok := ir.(*resp.MsgResponse)
	if !ok {
		a.FailNow("wrong type")
	}
	a.EqualValues(msgid, r.MsgId)
}

func VerifyItems(a *assert.Assertions, r apis.IResponse, size int, itemSpecs ...string) bool {
	a.Equal(size*2, len(itemSpecs))
	items, ok := VerifyItemsRespSize(a, r, size)
	if size*2 == len(itemSpecs) && ok {
		for i := 0; i < len(itemSpecs); i += 2 {
			itemPos := i / 2
			itemId := itemSpecs[i]
			itemPayload := itemSpecs[i+1]
			a.Equal(itemId, items[itemPos].StrId)
			a.Equal(itemPayload, string(items[itemPos].Payload))
		}
		return true
	}
	return false
}

func VerifyMsgIdResponse(a *assert.Assertions, msgID string, r apis.IResponse) bool {
	mr, ok := r.(*resp.MsgResponse)
	if a.True(ok) {
		return a.EqualValues(msgID, mr.MsgId)
	}
	return false
}
