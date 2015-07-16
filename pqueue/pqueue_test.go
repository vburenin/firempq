package pqueue

import (
	"firempq/db"
	"firempq/defs"
	"testing"
	"time"
)

func CreateTestQueue() *PQueue {
	ldb := db.GetDatabase()
	ldb.FlushCache()
	return NewPQueue(ldb, "name", 100, 1000)
}

func TestPushPopAndTimeUnlockItems(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.Push(map[string]string{
		defs.PRM_ID:       "data1",
		defs.PRM_PRIORITY: "12",
		defs.PRM_PAYLOAD:  "p1"})
	q.Push(map[string]string{
		defs.PRM_ID:       "data2",
		defs.PRM_PRIORITY: "12",
		defs.PRM_PAYLOAD:  "p2"})

	pop_msg1 := q.Pop(nil).Items[0]
	pop_msg2 := q.Pop(nil).Items[0]

	if pop_msg1.GetId() != "data1" {
		t.Error("Unexpected id. Expected 'data1' got: " + pop_msg1.GetId())
	}
	if pop_msg2.GetId() != "data2" {
		t.Error("Unexpected id. Expected 'data2' got: " + pop_msg2.GetId())
	}

	params := map[string]string{
		defs.PRM_ID:      "data1",
		defs.PRM_TIMEOUT: "10",
	}

	q.Call(ACTION_SET_LOCK_TIMEOUT, params)

	time.Sleep(110000000)

	pop_msg3 := q.Pop(nil).Items[0]
	if pop_msg3.GetId() != "data1" {
		t.Error("Unexpected id. Expected 'data1' got: " + pop_msg3.GetId())
	}
}

func TestAutoExpiration(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.settings.MsgTTL = 10
	q.Push(map[string]string{defs.PRM_ID: "dd1", defs.PRM_PRIORITY: "12", defs.PRM_PAYLOAD: "p1"})
	q.Push(map[string]string{defs.PRM_ID: "dd2", defs.PRM_PRIORITY: "12", defs.PRM_PAYLOAD: "p2"})

	// Wait for auto expiration.
	time.Sleep(130000000)
	msg := q.Pop(nil)
	if msg.Err == nil {
		t.Error("Unexpected message! It should be expired!")
	}
	if len(q.allMessagesMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}

func TestUnlockById(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()

	defer q.Close()
	defer q.Clear()

	q.Push(map[string]string{defs.PRM_ID: "dd1", defs.PRM_PRIORITY: "12", defs.PRM_PAYLOAD: "p1"})
	q.Push(map[string]string{defs.PRM_ID: "dd2", defs.PRM_PRIORITY: "12", defs.PRM_PAYLOAD: "p2"})

	q.Pop(nil)
	q.Pop(nil)

	params := map[string]string{defs.PRM_ID: "dd1"}
	q.Call(ACTION_UNLOCK_BY_ID, params)

	msg := q.Pop(nil).Items[0]
	if msg.GetId() != "dd1" {
		t.Error("Wrong message id is unlocked!")
	}
}

func TestDeleteById(t *testing.T) {
	q := CreateTestQueue()
	defer q.Close()
	defer q.Clear()

	q.Clear()

	q.Push(map[string]string{defs.PRM_ID: "dd1", defs.PRM_PRIORITY: "12", defs.PRM_PAYLOAD: "p1"})

	q.Call(ACTION_DELETE_BY_ID, map[string]string{defs.PRM_ID: "dd1"})

	msg := q.Pop(nil)
	if len(msg.Items) != 0 || msg.Err == nil {
		t.Error("Unexpected message! It should be expired!")
	}
	if len(q.allMessagesMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}

func TestDeleteLockedById(t *testing.T) {
	q := CreateTestQueue()
	defer q.Close()
	defer q.Clear()

	q.Clear()

	q.Push(map[string]string{defs.PRM_ID: "dd1", defs.PRM_PRIORITY: "12", defs.PRM_PAYLOAD: "p1"})

	params := map[string]string{defs.PRM_ID: "dd1"}
	res := q.Call(ACTION_DELETE_LOCKED_BY_ID, params)
	if res.Err == nil {
		t.Error("Non-locked item is unlocked!")
	}

	m := q.Pop(nil).Items[0]

	if m.GetId() != "dd1" {
		t.Error("Unexpected id!")
	}

	res = q.Call(ACTION_DELETE_LOCKED_BY_ID, params)
	if res.Err != nil {
		t.Error("Failed unlock!")
	}

	msg := q.Pop(nil)
	if msg.Err == nil {
		t.Error("Unexpected message! It should be deleted!")
	}
	if len(q.allMessagesMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}

func TestPopWaitBatch(t *testing.T) {
	q := CreateTestQueue()
	defer q.Close()
	defer q.Clear()

	q.Clear()

	go func() {
		time.Sleep(time.Second / 3)
		q.Push(map[string]string{defs.PRM_ID: "dd1", defs.PRM_PRIORITY: "12", defs.PRM_PAYLOAD: "p1"})
		q.Push(map[string]string{defs.PRM_ID: "dd2", defs.PRM_PRIORITY: "12", defs.PRM_PAYLOAD: "p2"})
		q.Push(map[string]string{defs.PRM_ID: "dd3", defs.PRM_PRIORITY: "12", defs.PRM_PAYLOAD: "p3"})
	}()

	params := map[string]string{defs.PRM_TIMEOUT: "100", defs.PRM_POP_LIMIT: "10"}
	m := q.Call(ACTION_POP_WAIT, params)
	if m.Err == nil {
		t.Error("No messages should be received! It have to timeout!")
	}

	// It is waiting for 1000 milliseconds so by this time we should receive 1 message.
	params = map[string]string{defs.PRM_TIMEOUT: "1200", defs.PRM_POP_LIMIT: "10"}
	m = q.Call(ACTION_POP_WAIT, params)

	if len(m.Items) == 0 {
		t.Error("No messages received!")
	} else {
		msgId := m.Items[0].GetId()
		if msgId != "dd1" {
			t.Error("Wrong message id!")
		}
		if len(m.Items) != 3 {
			t.Error("Number of received messages should be 3!")
		}
	}
}
