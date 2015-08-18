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
	return NewDSQueue(ldb, "dsqueue-test", 1000)
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

func TestLoadFromDb(t *testing.T) {

	q := CreateTestQueue()
	q.Clear()
	defer q.Close()
	defer q.Clear()

	q.PushBack(map[string]string{
		defs.PRM_ID:      "b1",
		defs.PRM_PAYLOAD: "p1"})
	q.PushFront(map[string]string{
		defs.PRM_ID:      "f1",
		defs.PRM_DELIVERY_INTERVAL: "500",
		defs.PRM_PAYLOAD: "p1"})
	q.PushBack(map[string]string{
		defs.PRM_ID:                "b2",
		defs.PRM_DELIVERY_INTERVAL: "500",
		defs.PRM_PAYLOAD:           "p2"})
	q.PushFront(map[string]string{
		defs.PRM_ID:      "f2",
		defs.PRM_PAYLOAD: "p1"})
	q.PushFront(map[string]string{
		defs.PRM_ID:      "f3",
		defs.PRM_PAYLOAD: "p1"})
	q.PopLockFront(nil)
	q.SetLockTimeout(map[string]string{
		defs.PRM_ID:      "f3",
		defs.PRM_TIMEOUT: "100"})
	// Wait till f3 will be unlocked and returned to the queue (priority front)
	time.Sleep(200 * time.Millisecond)
	defer q.Close()
	time.Sleep(100 * time.Millisecond)

	// Now reload queue from db as a new instance (should contain f3, f2, b1)
	ql := CreateTestQueue()
	defer ql.Close()
	defer ql.Clear()
	if ql.availableMsgs.Len() + ql.highPriorityFrontMsgs.Len()  != 3 {
		t.Error("Messages map should contain 4 messages instead of", ql.availableMsgs.Len() +
			ql.highPriorityFrontMsgs.Len() )
	}
	time.Sleep(300 * time.Millisecond)
	// Now f1 and b2 delivered and queue should contain 5 messages)
	if ql.availableMsgs.Len() + ql.highPriorityFrontMsgs.Len() != 5 {
		t.Error("Messages map should contain 4 messages instead of ", ql.availableMsgs.Len())
	}
	// Check order of loaded messages (front)
	msg := q.PopLockFront(nil).Items[0]
	if msg == nil || msg.GetId() != "f3" {
		t.Error("Messages order is wrong!")
	}
	msg = q.PopLockFront(nil).Items[0]
	if msg == nil || msg.GetId() != "f1" {
		t.Error("Messages order is wrong!")
	}
	msg = q.PopLockFront(nil).Items[0]
	if msg == nil || msg.GetId() != "f2" {
		t.Error("Messages order is wrong!")
	}
	// Check order of loaded messages (back)
	msg = q.PopLockBack(nil).Items[0]
	if msg == nil || msg.GetId() != "b2" {
		t.Error("Messages order is wrong!")
	}
	msg = q.PopLockBack(nil).Items[0]
	if msg == nil || msg.GetId() != "b1" {
		t.Error("Messages order is wrong!")
	}
}

