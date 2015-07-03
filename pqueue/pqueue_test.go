package pqueue

import (
	"firempq/defs"
	"testing"
	"time"
)

func TestPushPopAndTimeUnlockItems(t *testing.T) {
	q := NewPQueue(100, 10000)
	msg1 := NewPQMessage("data1", 12)
	msg2 := NewPQMessage("data2", 12)

	q.Push(msg1)
	q.Push(msg2)

	pop_msg1 := q.PopMessage()
	pop_msg2 := q.PopMessage()

	if pop_msg1.GetPayload() != "data1" {
		t.Error("Unexpected payload. Expected 'data1' got: " + pop_msg1.GetPayload())
	}
	if pop_msg2.GetPayload() != "data2" {
		t.Error("Unexpected payload. Expected 'data2' got: " + pop_msg2.GetPayload())
	}

	params := map[string]string{
		defs.PARAM_MSG_ID:      msg1.Id,
		defs.PARAM_MSG_TIMEOUT: "10",
	}

	q.CustomHandler(ACTION_SET_LOCK_TIMEOUT, params)

	time.Sleep(110000000)

	pop_msg3 := q.PopMessage()
	if pop_msg3.GetPayload() != "data1" {
		t.Error("Unexpected payload. Expected 'data1' got: " + pop_msg3.GetPayload())
	}
}

func TestAutoExpiration(t *testing.T) {
	q := NewPQueue(100, 10000)
	q.MsgTTL = 10
	msg1 := NewPQMessage("data1", 12)
	msg2 := NewPQMessage("data2", 12)

	q.Push(msg1)
	q.Push(msg2)

	// Wait for auto expiration.
	time.Sleep(110000000)
	msg := q.PopMessage()
	if msg != nil {
		t.Error("Unexpected message! It should be expired!")
	}
	if len(q.allMessagesMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}

func TestUnlockById(t *testing.T) {
	q := NewPQueue(100, 10000)
	msg1 := NewPQMessageWithId("id1", "data1", 12)
	msg2 := NewPQMessageWithId("id2", "data2", 12)

	q.Push(msg1)
	q.Push(msg2)

	q.PopMessage()
	q.PopMessage()

	params := map[string]string{defs.PARAM_MSG_ID: msg1.Id}
	q.CustomHandler(ACTION_UNLOCK_BY_ID, params)

	msg := q.PopMessage()
	if msg.Id != "id1" {
		t.Error("Wrong message id is unlocked!")
	}
}

func TestDeleteById(t *testing.T) {
	q := NewPQueue(100, 10000)
	msg1 := NewPQMessageWithId("id1", "data1", 12)
	q.Push(msg1)

	q.DeleteById("id1")

	msg := q.PopMessage()
	if msg != nil {
		t.Error("Unexpected message! It should be expired!")
	}
	if len(q.allMessagesMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}

func TestDeleteLockedById(t *testing.T) {
	q := NewPQueue(100, 10000)
	msg1 := NewPQMessageWithId("id1", "data1", 12)
	q.Push(msg1)

	params := map[string]string{defs.PARAM_MSG_ID: msg1.Id}
	res := q.CustomHandler(ACTION_DELETE_LOCKED_BY_ID, params)
	if res == nil {
		t.Error("Not locked item is unlocked!")
	}

	q.PopMessage()
	res = q.CustomHandler(ACTION_DELETE_LOCKED_BY_ID, params)
	if res != nil {
		t.Error("Failed unlock!")
	}

	msg := q.PopMessage()
	if msg != nil {
		t.Error("Unexpected message! It should be deleted!")
	}
	if len(q.allMessagesMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}
