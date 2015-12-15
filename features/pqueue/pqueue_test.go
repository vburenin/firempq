package pqueue

import (
	"firempq/common"
	"firempq/db"
	"firempq/testutils"
	"testing"
	"time"
)

func CreateTestQueue() *PQueue {
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

	q.Clear()
	defer q.Close()
	defer q.Clear()
	q.Push("data1", "p1", 0, 12)
	q.Push("data2", "p2", 0, 12)

	popMsg1 := q.Pop(10000, 0, 1).GetResponse()
	popMsg2 := q.Pop(10000, 0, 1).GetResponse()

	cmp(t, popMsg1, "+DATA *1 %2 ID $5 data1 PL $2 p1")
	cmp(t, popMsg2, "+DATA *1 %2 ID $5 data2 PL $2 p2")

	q.SetLockTimeout("data1", 0)
	q.update(common.Uts() + 110)

	popMsg3 := q.Pop(10000, 0, 1).GetResponse()
	cmp(t, popMsg3, "+DATA *1 %2 ID $5 data1 PL $2 p1")
}

func TestAutoExpiration(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.config.MsgTtl = 10
	q.Push("dd1", "p1", 0, 12)
	q.Push("dd2", "p2", 0, 12)

	// Wait for auto expiration.
	q.update(common.Uts() + 1300)
	msg := q.Pop(10000, 0, 10).GetResponse()
	cmp(t, msg, "+DATA *0")
	if len(q.msgMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}

func TestUnlockById(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()

	defer q.Close()
	defer q.Clear()

	q.Push("dd1", "p1", 0, 12)
	q.Push("dd2", "p2", 0, 12)

	q.Pop(10000, 0, 10)
	q.Pop(10000, 0, 10)

	q.UnlockMessageById("dd1")

	msg := q.Pop(10000, 0, 10).GetResponse()
	cmp(t, msg, "+DATA *1 %2 ID $3 dd1 PL $2 p1")
}

func TestDeleteById(t *testing.T) {
	q := CreateTestQueue()
	defer q.Close()
	defer q.Clear()

	q.Clear()

	q.Push("dd1", "p1", 0, 12)

	cmp(t, q.DeleteById("dd1").GetResponse(), "+OK")

	msg := q.Pop(10000, 0, 10).GetResponse()
	cmp(t, msg, "+DATA *0")

	if len(q.msgMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}

func TestDeleteLockedById(t *testing.T) {
	q := CreateTestQueue()
	defer q.Close()
	defer q.Clear()

	q.Clear()

	q.Push("dd1", "p1", 0, 12)

	res := q.DeleteLockedById("dd1")
	if !res.IsError() {
		t.Error("Non-locked item is unlocked!")
	}

	m := q.Pop(1000, 0, 10).GetResponse()
	cmp(t, m, "+DATA *1 %2 ID $3 dd1 PL $2 p1")

	res = q.DeleteLockedById("dd1")
	cmp(t, res.GetResponse(), "+OK")

	msg := q.Pop(10000, 0, 10).GetResponse()
	cmp(t, msg, "+DATA *0")
	if len(q.msgMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}

func TestPopWaitBatch(t *testing.T) {
	q := CreateTestQueue()
	defer q.Close()
	defer q.Clear()

	q.Clear()

	go func() {
		time.Sleep(time.Second / 10)
		q.Push("dd1", "p1", 0, 12)
		q.Push("dd2", "p2", 0, 12)
		q.Push("dd3", "p3", 0, 12)
	}()

	m := q.Pop(10000, 1, 10).GetResponse()
	cmp(t, m, "+DATA *0")

	// It is waiting for 1000 milliseconds so by this time we should receive 1 message.
	m = q.Pop(10000, 1200, 10).GetResponse()
	cmp(t, m, "+DATA *3 %2 ID $3 dd1 PL $2 p1 %2 ID $3 dd2 PL $2 p2 %2 ID $3 dd3 PL $2 p3")
}
