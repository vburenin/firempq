package dsqueue

import (
	"firempq/db"
	"firempq/defs"
	"testing"
	"time"
)

func CreateTestQueue() *DSQueue {
	ldb := db.GetDatabase()
	ldb.FlushCache()
	return NewDSQueue(ldb, "", 1000)
}

func TestDelete(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushFront(map[string]string{
		defs.PRM_ID:      "data1",
		defs.PRM_PAYLOAD: "p1"})
	q.PushFront(map[string]string{
		defs.PRM_ID:      "data2",
		defs.PRM_PAYLOAD: "p2"})

	q.DeleteById(map[string]string{defs.PRM_ID: "data1"})

	pop_msg1 := q.PopLockFront(nil).Items[0]
	if pop_msg1.GetId() != "data2" {
		t.Error("Unexpected id. Expected 'data2' got: " + pop_msg1.GetId())
	}

	err := q.DeleteById(map[string]string{defs.PRM_ID: "data1"})
	if err.Err == nil {
		t.Error("Locked message war deleted by 'DeleteById'")
	}

	err = q.DeleteLockedById(map[string]string{defs.PRM_ID: pop_msg1.GetId()})
	if err.Err != nil {
		t.Error("Failed to delete Locked message")
	}
}

func TestPushFront(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushFront(map[string]string{
		defs.PRM_ID:      "data1",
		defs.PRM_PAYLOAD: "p1"})
	q.PushFront(map[string]string{
		defs.PRM_ID:      "data2",
		defs.PRM_PAYLOAD: "p2"})

	pop_msg1 := q.PopFront(nil).Items[0]
	pop_msg2 := q.PopFront(nil).Items[0]

	if pop_msg1.GetId() != "data2" {
		t.Error("Unexpected id. Expected 'data2' got: " + pop_msg1.GetId())
	}
	if pop_msg2.GetId() != "data1" {
		t.Error("Unexpected id. Expected 'data1' got: " + pop_msg2.GetId())
	}
}

func TestPushFrontDelayed(t *testing.T) {
	q := CreateTestQueue()
	q.SetTestLog(t)
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushFront(map[string]string{
		defs.PRM_ID:      "data1",
		defs.PRM_PAYLOAD: "p1"})
	q.PushFront(map[string]string{
		defs.PRM_ID:                "data2",
		defs.PRM_DELIVERY_INTERVAL: "100",
		defs.PRM_PAYLOAD:           "p2"})

	time.Sleep(50 * time.Millisecond)
	pop_msg1 := q.PopFront(nil).Items[0]
	time.Sleep(200 * time.Millisecond)
	pop_msg2 := q.PopFront(nil).Items[0]

	if pop_msg1 != nil && pop_msg1.GetId() != "data1" {
		t.Error("Unexpected id. Expected 'data1' got: " + pop_msg1.GetId())
	}
	if pop_msg2 != nil && pop_msg2.GetId() != "data2" {
		t.Error("Unexpected id. Expected 'data2' got: " + pop_msg2.GetId())
	}
}

func TestPushBack(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushBack(map[string]string{
		defs.PRM_ID:      "data1",
		defs.PRM_PAYLOAD: "p1"})
	q.PushBack(map[string]string{
		defs.PRM_ID:      "data2",
		defs.PRM_PAYLOAD: "p2"})

	pop_msg1 := q.PopBack(nil).Items[0]
	pop_msg2 := q.PopBack(nil).Items[0]

	if pop_msg1.GetId() != "data2" {
		t.Error("Unexpected id. Expected 'data1' got: " + pop_msg1.GetId())
	}
	if pop_msg2.GetId() != "data1" {
		t.Error("Unexpected id. Expected 'data2' got: " + pop_msg2.GetId())
	}
}
func TestUnlockById(t *testing.T) {
}

func TestPushBackDelayed(t *testing.T) {
	q := CreateTestQueue()
	q.SetTestLog(t)
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushBack(map[string]string{
		defs.PRM_ID:      "data1",
		defs.PRM_PAYLOAD: "p1"})
	q.PushBack(map[string]string{
		defs.PRM_ID:                "data2",
		defs.PRM_DELIVERY_INTERVAL: "100",
		defs.PRM_PAYLOAD:           "p2"})

	time.Sleep(50 * time.Millisecond)
	pop_msg1 := q.PopFront(nil).Items[0]
	time.Sleep(200 * time.Millisecond)
	pop_msg2 := q.PopFront(nil).Items[0]

	if pop_msg1 != nil && pop_msg1.GetId() != "data1" {
		t.Error("Unexpected id. Expected 'data1' got: " + pop_msg1.GetId())
	}
	if pop_msg2 != nil && pop_msg2.GetId() != "data2" {
		t.Error("Unexpected id. Expected 'data2' got: " + pop_msg1.GetId())
	}
}

func TestAutoExpiration(t *testing.T) {
	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.settings.MsgTTL = 10
	q.PushFront(map[string]string{defs.PRM_ID: "dd1", defs.PRM_PRIORITY: "12", defs.PRM_PAYLOAD: "p1"})
	q.PushBack(map[string]string{defs.PRM_ID: "dd2", defs.PRM_PRIORITY: "12", defs.PRM_PAYLOAD: "p2"})

	// Wait for auto expiration.
	time.Sleep(2000 * time.Millisecond)
	msg := q.PopFront(nil)
	if msg.Err == nil {
		t.Error("Unexpected message! It should be expired!")
	}
	msg = q.PopBack(nil)
	if msg.Err == nil {
		t.Error("Unexpected message! It should be expired!")
	}
	if len(q.allMessagesMap) != 0 {
		t.Error("Messages map must be empty!")
	}
}

func TestLockAndReturn(t *testing.T) {
	q := CreateTestQueue()
	q.SetTestLog(t)
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushBack(map[string]string{
		defs.PRM_ID:      "data1",
		defs.PRM_PAYLOAD: "p1"})
	q.PushBack(map[string]string{
		defs.PRM_ID:                "data2",
		defs.PRM_DELIVERY_INTERVAL: "10",
		defs.PRM_PAYLOAD:           "p2"})

	time.Sleep(50 * time.Millisecond)
	if q.availableMsgs.Len() != 2 {
		t.Error("Messages map should contain 2 messages!")
	}

	msg1 := q.PopLockFront(nil)
	if msg1.Err != nil {
		t.Error("No message 1 in a queue!")
	}
	msg2 := q.PopLockFront(nil)
	if msg2.Err != nil {
		t.Error("No message 2 in a queue!", msg2.Err)
	}

	time.Sleep(1500 * time.Millisecond)

	msg1 = q.PopFront(nil)
	if msg1.Err != nil {
		t.Error("Message not returned to a queue!")
	}
	msg2 = q.PopFront(nil)
	if msg2.Err != nil {
		t.Error("Message not returned to a queue!")
	}
}

func TestDeleteById(t *testing.T) {
}
