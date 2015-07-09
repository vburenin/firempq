package pqueue

import (
	"firempq/defs"
	"testing"
	"time"
)

func TestPushPopAndTimeUnlockItems(t *testing.T) {
	q := CreatePQueue("name", nil)
	q.DeleteAll()
	defer q.Close()
	defer q.DeleteAll()

	msg1 := NewPQMessageWithId("data1", 12)
	msg2 := NewPQMessageWithId("data2", 12)

	q.Push(msg1, "data1")
	q.Push(msg2, "data2")

	pop_msg1 := q.Pop()
	pop_msg2 := q.Pop()

	if pop_msg1.GetId() != "data1" {
		t.Error("Unexpected id. Expected 'data1' got: " + pop_msg1.GetId())
	}
	if pop_msg2.GetId() != "data2" {
		t.Error("Unexpected id. Expected 'data2' got: " + pop_msg2.GetId())
	}

	params := map[string]string{
		defs.PARAM_MSG_ID:      msg1.Id,
		defs.PARAM_MSG_TIMEOUT: "10",
	}

	q.CustomHandler(ACTION_SET_LOCK_TIMEOUT, params)

	time.Sleep(110000000)

	pop_msg3 := q.Pop()
	if pop_msg3.GetId() != "data1" {
		t.Error("Unexpected id. Expected 'data1' got: " + pop_msg3.GetId())
	}
}

func TestAutoExpiration(t *testing.T) {
	q := CreatePQueue("name", nil)
	q.DeleteAll()
	defer q.Close()
	defer q.DeleteAll()

	q.settings.MsgTTL = 10
	msg1 := NewPQMessageWithId("data1", 12)
	msg2 := NewPQMessageWithId("data2", 12)

	q.Push(msg1, "data1")
	q.Push(msg2, "data2")

	// Wait for auto expiration.
	time.Sleep(130000000)
	msg := q.Pop()
	if msg != nil {
		t.Error("Unexpected message! It should be expired!")
	}
	if len(q.allMessagesMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}

func TestUnlockById(t *testing.T) {
	q := CreatePQueue("name", nil)
	q.DeleteAll()

	defer q.Close()
	defer q.DeleteAll()

	msg1 := NewPQMessageWithId("id1", 12)
	msg2 := NewPQMessageWithId("id2", 12)

	q.Push(msg1, "data1")
	q.Push(msg2, "data2")

	q.Pop()
	q.Pop()

	params := map[string]string{defs.PARAM_MSG_ID: msg1.Id}
	q.CustomHandler(ACTION_UNLOCK_BY_ID, params)

	msg := q.Pop()
	if msg.Id != "id1" {
		t.Error("Wrong message id is unlocked!")
	}
}

func TestDeleteById(t *testing.T) {
	q := CreatePQueue("name", nil)
	defer q.Close()
	defer q.DeleteAll()

	q.DeleteAll()

	msg1 := NewPQMessageWithId("id1", 12)
	q.Push(msg1, "data1")

	q.DeleteById("id1")

	msg := q.Pop()
	if msg != nil {
		t.Error("Unexpected message! It should be expired!")
	}
	if len(q.allMessagesMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}

func TestDeleteLockedById(t *testing.T) {
	q := CreatePQueue("name", nil)
	defer q.Close()
	defer q.DeleteAll()

	q.DeleteAll()

	msg1 := NewPQMessageWithId("id1", 12)
	q.Push(msg1, "data1")

	params := map[string]string{defs.PARAM_MSG_ID: msg1.Id}
	res := q.CustomHandler(ACTION_DELETE_LOCKED_BY_ID, params)
	if res == nil {
		t.Error("Not locked item is unlocked!")
	}

	q.Pop()
	res = q.CustomHandler(ACTION_DELETE_LOCKED_BY_ID, params)
	if res != nil {
		t.Error("Failed unlock!")
	}

	msg := q.Pop()
	if msg != nil {
		t.Error("Unexpected message! It should be deleted!")
	}
	if len(q.allMessagesMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}
