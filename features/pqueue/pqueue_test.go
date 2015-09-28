package pqueue

import (
	"firempq/common"
	"firempq/db"
	"testing"
	"time"
)

func CreateTestQueue() *PQueue {
	ldb := db.GetDatabase()
	ldb.FlushCache()
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

	q.Push([]string{PRM_ID, "data1", PRM_PRIORITY, "12", PRM_PAYLOAD, "p1"})
	q.Push([]string{PRM_ID, "data2", PRM_PRIORITY, "12", PRM_PAYLOAD, "p2"})

	pop_msg1 := q.Pop([]string{}).GetResponse()
	pop_msg2 := q.Pop([]string{}).GetResponse()

	cmp(t, pop_msg1, "+DATA *1 %2 ID $5 data1 PL $2 p1")
	cmp(t, pop_msg2, "+DATA *1 %2 ID $5 data2 PL $2 p2")

	params := []string{PRM_ID, "data1", PRM_LOCK_TIMEOUT, "0"}

	q.Call(ACTION_SET_LOCK_TIMEOUT, params)

	q.update(common.Uts() + 110000)

	pop_msg3 := q.Pop(nil).GetResponse()
	cmp(t, pop_msg3, "+DATA *1 %2 ID $5 data1 PL $2 p1")
}

func TestAutoExpiration(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.config.MsgTtl = 10
	q.Push([]string{PRM_ID, "dd1", PRM_PRIORITY, "12", PRM_PAYLOAD, "p1"})
	q.Push([]string{PRM_ID, "dd2", PRM_PRIORITY, "12", PRM_PAYLOAD, "p2"})

	// Wait for auto expiration.
	q.update(common.Uts() + 1300)
	msg := q.Pop([]string{}).GetResponse()
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

	q.Push([]string{PRM_ID, "dd1", PRM_PRIORITY, "12", PRM_PAYLOAD, "p1"})
	q.Push([]string{PRM_ID, "dd2", PRM_PRIORITY, "12", PRM_PAYLOAD, "p2"})

	q.Pop([]string{})
	q.Pop([]string{})

	params := []string{PRM_ID, "dd1"}
	q.Call(ACTION_UNLOCK_BY_ID, params)

	msg := q.Pop(nil).GetResponse()
	cmp(t, msg, "+DATA *1 %2 ID $3 dd1 PL $2 p1")
}

func TestDeleteById(t *testing.T) {
	q := CreateTestQueue()
	defer q.Close()
	defer q.Clear()

	q.Clear()

	q.Push([]string{PRM_ID, "dd1", PRM_PRIORITY, "12", PRM_PAYLOAD, "p1"})

	cmp(t, q.Call(ACTION_DELETE_BY_ID, []string{PRM_ID, "dd1"}).GetResponse(), "+OK")

	msg := q.Pop(nil).GetResponse()
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

	q.Push([]string{PRM_ID, "dd1", PRM_PRIORITY, "12", PRM_PAYLOAD, "p1"})

	params := []string{PRM_ID, "dd1"}
	res := q.Call(ACTION_DELETE_LOCKED_BY_ID, params)
	if !res.IsError() {
		t.Error("Non-locked item is unlocked!")
	}

	m := q.Pop(nil).GetResponse()
	cmp(t, m, "+DATA *1 %2 ID $3 dd1 PL $2 p1")

	res = q.Call(ACTION_DELETE_LOCKED_BY_ID, params)
	cmp(t, res.GetResponse(), "+OK")

	msg := q.Pop(nil).GetResponse()
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
		q.Push([]string{PRM_ID, "dd1", PRM_PRIORITY, "12", PRM_PAYLOAD, "p1"})
		q.Push([]string{PRM_ID, "dd2", PRM_PRIORITY, "12", PRM_PAYLOAD, "p2"})
		q.Push([]string{PRM_ID, "dd3", PRM_PRIORITY, "12", PRM_PAYLOAD, "p3"})
	}()

	params := []string{PRM_POP_WAIT_TIMEOUT, "1", PRM_LIMIT, "10"}

	m := q.Call(ACTION_POP_WAIT, params).GetResponse()
	cmp(t, m, "+DATA *0")

	// It is waiting for 1000 milliseconds so by this time we should receive 1 message.
	params = []string{PRM_POP_WAIT_TIMEOUT, "1200", PRM_LIMIT, "10"}
	m = q.Call(ACTION_POP_WAIT, params).GetResponse()
	cmp(t, m, "+DATA *3 %2 ID $3 dd1 PL $2 p1 %2 ID $3 dd2 PL $2 p2 %2 ID $3 dd3 PL $2 p3")
}
