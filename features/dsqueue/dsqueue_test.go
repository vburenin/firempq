package dsqueue

import (
	"firempq/db"
	"strings"
	"testing"
	"time"
)

func CreateTestQueue() *DSQueue {
	ldb := db.GetDatabase()
	ldb.FlushCache()
	return NewDSQueue(ldb, "dsqueue-test", 1000)
}

func TestDelete(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushFront([]string{PRM_ID, "data1", PRM_PAYLOAD, "p1"})
	q.PushFront([]string{PRM_ID, "data2", PRM_PAYLOAD, "p2"})

	resp := q.DeleteById([]string{PRM_ID, "data1"})
	if resp.GetResponse() != "+OK:200" {
		t.Error("Unexpected response")
	}

	pop_msg1 := q.PopLockFront(nil).GetResponse()
	expected1 := "+DATA *1 $5 data2$2 p2"
	if pop_msg1 != expected1 {
		t.Error("Unexpected data. Expected: '" + expected1 + "'received: '" + pop_msg1 + "'")

	}

	err := q.DeleteById([]string{PRM_ID, "data1"})
	if !err.IsError() {
		t.Error("Locked message was deleted by 'DeleteById'")
	}

	err = q.DeleteLockedById([]string{PRM_ID, "data2"})
	if err.IsError() {
		t.Error("Failed to delete Locked message")
	}
}

func TestPushFront(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushFront([]string{PRM_ID, "data1", PRM_PAYLOAD, "p1"})
	q.PushFront([]string{PRM_ID, "data2", PRM_PAYLOAD, "p2"})

	pop_msg1 := q.PopFront(nil).GetResponse()
	pop_msg2 := q.PopFront(nil).GetResponse()

	if pop_msg1 != "+DATA *1 $5 data2$2 p2" {
		t.Error("Unexpected id. Expected 'data2' got: " + pop_msg1)
	}
	if pop_msg2 != "+DATA *1 $5 data1$2 p1" {
		t.Error("Unexpected id. Expected 'data1' got: " + pop_msg2)
	}
}

func TestPushFrontDelayed(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushFront([]string{PRM_ID, "data1", PRM_PAYLOAD, "p1"})
	q.PushFront([]string{PRM_ID, "data2", PRM_DELIVERY_DELAY, "100", PRM_PAYLOAD, "p2"})

	time.Sleep(50 * time.Millisecond)
	pop_msg1 := q.PopFront(nil).GetResponse()
	time.Sleep(200 * time.Millisecond)
	pop_msg2 := q.PopFront(nil).GetResponse()

	if pop_msg1 != "+DATA *1 $5 data1$2 p1" {
		t.Error("Unexpected id. Expected 'data1' got: " + pop_msg1)
	}
	if pop_msg2 != "+DATA *1 $5 data2$2 p2" {
		t.Error("Unexpected id. Expected 'data2' got: " + pop_msg2)
	}
}

func TestPushBack(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushBack([]string{PRM_ID, "data1", PRM_PAYLOAD, "p1"})
	q.PushBack([]string{PRM_ID, "data2", PRM_PAYLOAD, "p2"})

	pop_msg1 := q.PopBack(nil).GetResponse()
	pop_msg2 := q.PopBack(nil).GetResponse()

	if pop_msg1 != "+DATA *1 $5 data2$2 p2" {
		t.Error("Unexpected id. Expected 'data2' got: " + pop_msg1)
	}
	if pop_msg2 != "+DATA *1 $5 data1$2 p1" {
		t.Error("Unexpected id. Expected 'data1' got: " + pop_msg2)
	}
}

func TestPushBackDelayed(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushBack([]string{PRM_ID, "data1", PRM_PAYLOAD, "p1"})
	q.PushBack([]string{PRM_ID, "data2", PRM_DELIVERY_DELAY, "100", PRM_PAYLOAD, "p2"})

	time.Sleep(50 * time.Millisecond)
	pop_msg1 := q.PopFront(nil).GetResponse()
	time.Sleep(200 * time.Millisecond)
	pop_msg2 := q.PopFront(nil).GetResponse()

	if pop_msg1 != "+DATA *1 $5 data1$2 p1" {
		t.Error("Unexpected id. Expected 'data1' got: " + pop_msg1)
	}
	if pop_msg2 != "+DATA *1 $5 data2$2 p2" {
		t.Error("Unexpected id. Expected 'data2' got: " + pop_msg1)
	}
}

func TestAutoExpiration(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.settings.MsgTTL = 10
	q.PushFront([]string{PRM_ID, "dd1", PRM_PAYLOAD, "p1"})
	q.PushBack([]string{PRM_ID, "dd2", PRM_PAYLOAD, "p2"})

	// Wait for auto expiration.
	time.Sleep(2000 * time.Millisecond)
	msg := q.PopFront(nil).GetResponse()
	if msg != "+DATA *0" {
		t.Error("Unexpected message It should be expired!")
	}
	msg = q.PopBack(nil).GetResponse()
	if msg != "+DATA *0" {
		t.Error("Unexpected message It should be expired!")
	}
	if len(q.allMessagesMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}

func TestLockAndReturn(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushBack([]string{PRM_ID, "data1", PRM_PAYLOAD, "p1"})
	q.PushBack([]string{PRM_ID, "data2", PRM_DELIVERY_DELAY, "10", PRM_PAYLOAD, "p2"})

	time.Sleep(50 * time.Millisecond)
	if q.availableMsgs.Len() != 2 {
		t.Error("Messages map should contain 2 messages!")
	}

	msg1 := q.PopLockFront(nil).GetResponse()
	msg2 := q.PopLockFront(nil).GetResponse()

	if msg1 != "+DATA *1 $5 data1$2 p1" {
		t.Error("Unexpected data: ", msg1)
	}

	if msg2 != "+DATA *1 $5 data2$2 p2" {
		t.Error("Unexpected data: ", msg2)
	}

	time.Sleep(1500 * time.Millisecond)

	msg3 := q.PopFront(nil)
	if msg3.IsError() {
		t.Error("Message not returned to a queue!")
	}
	msg4 := q.PopFront(nil)
	if msg4.IsError() {
		t.Error("Message not returned to a queue!")
	}
}

func TestLoadFromDb(t *testing.T) {

	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushBack([]string{PRM_ID, "b1", PRM_PAYLOAD, "p1"})
	q.PushBack([]string{PRM_ID, "b2", PRM_DELIVERY_DELAY, "500", PRM_PAYLOAD, "p2"})
	q.PushFront([]string{PRM_ID, "f1", PRM_DELIVERY_DELAY, "500", PRM_PAYLOAD, "p1"})
	q.PushFront([]string{PRM_ID, "f2", PRM_PAYLOAD, "p1"})
	q.PushFront([]string{PRM_ID, "f3", PRM_PAYLOAD, "p1"})
	q.PopLockFront(nil)
	q.SetLockTimeout([]string{PRM_ID, "f3", PRM_LOCK_TIMEOUT, "100"})
	// Wait till f3 will be unlocked and returned to the queue (priority front)
	time.Sleep(200 * time.Millisecond)
	q.Close()
	time.Sleep(100 * time.Millisecond)

	// Now reload queue from db as a new instance (should contain f3, f2, b1)
	ql := CreateTestQueue()
	defer ql.Close()
	defer ql.Clear()
	if ql.availableMsgs.Len()+ql.highPriorityFrontMsgs.Len() != 3 {
		t.Error("Messages map should contain 3 messages instead of", ql.availableMsgs.Len()+
			ql.highPriorityFrontMsgs.Len())
	}
	time.Sleep(300 * time.Millisecond)
	// Now f1 and b2 delivered and queue should contain 5 messages)
	if ql.availableMsgs.Len()+ql.highPriorityFrontMsgs.Len() != 5 {
		t.Error("Messages map should contain 5 messages instead of ", ql.availableMsgs.Len())
	}
	// Check order of loaded messages (front)
	msg := ql.PopLockFront(nil).GetResponse()

	if !strings.Contains(msg, "f3") {
		t.Error("Messages order is wrong! Got", msg, "instead of f3")
	}
	msg = ql.PopLockFront(nil).GetResponse()
	if !strings.Contains(msg, "f1") {
		t.Error("Messages order is wrong! Got", msg, "instead of f1")
	}
	msg = ql.PopLockFront(nil).GetResponse()
	if !strings.Contains(msg, "f2") {
		t.Error("Messages order is wrong! Got", msg, "instead of f2")
	}

	// Check order of loaded messages (back)
	msg = ql.PopLockBack(nil).GetResponse()
	if !strings.Contains(msg, "b2") {
		t.Error("Messages order is wrong! Got", msg, "instead of b2")
	}
	msg = ql.PopLockBack(nil).GetResponse()
	if !strings.Contains(msg, "b1") {
		t.Error("Messages order is wrong! Got", msg, "instead of b1")
	}
}
