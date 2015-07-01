package pqueue

import (
    "firempq/qerrors"
    "firempq/structs"
    "firempq/queues/priority_first"
	"log"
	"sync"
	"time"
)

const (
	DEFAULT_TTL             = 3600 * 24 * 14
	DEFAULT_DELIVERY_DELAY  = 0
	DEFAULT_LOCK_TIMEOUT    = 300000
	DEFAULT_UNLOCK_INTERVAL = 1000 * 1000000 // 1 second
    DEFAULT_POP_COUNT_LIMIT = 0 // Unlimited.
)

type PQueue struct {
	// Messages which are waiting to be picked up
	availableMsgs *priority_first.PriorityFirstQueue

	// All messages with the ticking counters except those which are inFlight.
	expireHeap *structs.IndexHeap
	// All locked messages
	inFlightHeap *structs.IndexHeap
	// Just a message map message id to the full message data.
	allMessagesMap map[string]*PQMessage

	MsgTTL        int64
	DeliveryDelay int64
	PopLockTimeout   int64
    PopCountLimit int64

	MaxSize     int64 // Max queue size.
	CreateTs    int64  // Time when queue was created.
	LastPushTs   int64  // Last time item has been pushed into queue.
	LastPopTs   int64  // Last pop time.
	MaxPriority int64
	lock        sync.Mutex
	workDone    bool
}

func Uts() int64 {
	return time.Now().UnixNano() / 1000000
}

func NewPQueue(maxPriority int64, maxSize int64) *PQueue {
	// Covert to milliseconds.
	uts := Uts()

	pq := PQueue{
		MaxPriority:  maxPriority,
		MaxSize:      maxSize,
		CreateTs:     uts,
		LastPushTs:    0,
		LastPopTs:    0,
		inFlightHeap: structs.NewIndexHeap(),
		expireHeap:   structs.NewIndexHeap(),
		workDone:     false,
	}
	pq.MsgTTL = DEFAULT_TTL
	pq.DeliveryDelay = DEFAULT_DELIVERY_DELAY
	pq.PopLockTimeout = DEFAULT_LOCK_TIMEOUT
    pq.PopCountLimit = DEFAULT_POP_COUNT_LIMIT

	pq.availableMsgs = priority_first.NewActiveQueues(maxPriority)
	pq.allMessagesMap = make(map[string]*PQMessage)
	go pq.periodicCleanUp()
	return &pq
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (pq *PQueue) PushMessage(msg *PQMessage) error {
	pq.lock.Lock()
	defer pq.lock.Unlock()
    pq.LastPushTs = Uts()

	if _, ok := pq.allMessagesMap[msg.Id]; ok {
		return qerrors.ERR_ITEM_ALREADY_EXISTS
	}
    pq.trackExpiration(msg)
    pq.allMessagesMap[msg.Id] = msg
    pq.availableMsgs.Push(msg.Id, msg.Priority)
	return nil
}

// Pop first available message.
// Will return nil if there are no messages available.
func (pq *PQueue) PopMessage() *PQMessage {
	pq.lock.Lock()
	defer pq.lock.Unlock()
    pq.LastPopTs = Uts()

	if pq.availableMsgs.Empty() {
		return nil
	}
	msgId := pq.availableMsgs.Pop()
	pqMsg := pq.allMessagesMap[msgId]

    // Increase number of pop attempts.
    pqMsg.PopCount += 1

	pq.expireHeap.PopById(msgId)
	pq.inFlightHeap.PushItem(msgId, Uts()+pq.PopLockTimeout)

	return pqMsg
}

// Remove message id from In Flight message heap.
func (pq *PQueue) unflightMessage(msgId string) (*PQMessage, error) {
    msg, ok := pq.allMessagesMap[msgId];
    if !ok {
        return nil, qerrors.ERR_MSG_NOT_EXIST
    }

    hi := pq.inFlightHeap.PopById(msgId)
    if hi == structs.EMPTY_HEAP_ITEM {
        return nil, qerrors.ERR_MSG_NOT_LOCKED
    }

    return msg, nil
}

// Remove locked message by id.
func (pq *PQueue) RemoveLockedById(msgId string) error {
    pq.lock.Lock()
    defer pq.lock.Unlock()

    _, err := pq.unflightMessage(msgId)
    if err != nil {
        return err
    }
    delete(pq.allMessagesMap, msgId)
    return nil
}

func (pq *PQueue) RemoveById(msgId string) error {
    pq.lock.Lock()
    defer pq.lock.Unlock()

    if pq.inFlightHeap.ContainsId(msgId) {
        return qerrors.ERR_MSG_IS_LOCKED
    }

    delete(pq.allMessagesMap, msgId)
    return nil
}

// Set a user defined message lock timeout. Only locked message timeout can be set.
func (pq *PQueue) SetLockTimeout(msgId string, timeout int64) error {
    pq.lock.Lock()
    defer pq.lock.Unlock()

    _, err := pq.unflightMessage(msgId)
    if err != nil {
        return err
    }
    pq.inFlightHeap.PushItem(msgId, Uts()+timeout)
    return nil
}

func (pq *PQueue) UnlockMessageById(msgId string) error {
    pq.lock.Lock()
    defer pq.lock.Unlock()

    // Make sure message exists.
    msg, err := pq.unflightMessage(msgId)
    if err != nil {
        return err
    }
    // Message exists, push it into the front of the queue.
    return pq.returnToFront(msg)
}

// Adds message into expiration heap. Not thread safe!
func (pq *PQueue) trackExpiration(msg *PQMessage) {
    ok := pq.expireHeap.PushItem(msg.Id, msg.CreatedTs+int64(pq.MsgTTL))
    if !ok {
        log.Println("Error! Item already exists in the expire heap: ", msg.Id)
    }
}

// Attempts to return a message into the front of the queue.
// If a number of POP attempts has exceeded, message will be removed completely.
func (pq *PQueue) returnToFront(msg *PQMessage) error {
    if pq.PopCountLimit > 0 {
        if pq.PopCountLimit <= msg.PopCount {
            delete(pq.allMessagesMap, msg.Id)
            return qerrors.ERR_MSG_POP_ATTEMPTS_EXCEEDED
        }
    }
    msg.Priority = 0
    pq.availableMsgs.PushFront(msg.Id)
    pq.trackExpiration(msg)
    return nil
}

// Unlocks all items which exceeded their lock time.
func (pq *PQueue) releaseInFlight() int {
    cur_ts := Uts()
    ifHeap := pq.inFlightHeap

    pq.lock.Lock()
    defer pq.lock.Unlock()

    i := 0
    for !(ifHeap.Empty()) && ifHeap.MinElement() < cur_ts {
        i++
        hi := ifHeap.PopItem()
        pqmsg := pq.allMessagesMap[hi.Id]
        pq.returnToFront(pqmsg)
    }
    return i
}

// Remove all items which are completely expired.
func (pq *PQueue) cleanExpiredItems() int {
    cur_ts := Uts()
    eh := pq.expireHeap

    pq.lock.Lock()
    defer pq.lock.Unlock()
    i := 0
    for !(eh.Empty()) && eh.MinElement() < cur_ts {
        i++
        hi := eh.PopItem()
        // There are two places to remove expired item:
        // 1. Map of all items - all items map.
        // 2. Available message.
        pqmsg := pq.allMessagesMap[hi.Id]
        pq.availableMsgs.RemoveItem(pqmsg.Id, pqmsg.Priority)
        delete(pq.allMessagesMap, pqmsg.Id)
    }
    return i
}

// Remove expired items.
func (pq *PQueue) periodicCleanUp() {
    for !(pq.workDone) {
        if cleaned := pq.releaseInFlight(); cleaned > 0 {
            log.Println(cleaned, "messages returned to the front of the queue.")
        }
        if cleaned := pq.cleanExpiredItems(); cleaned > 0 {
            log.Println(cleaned, "items expired.")
        }
        time.Sleep(DEFAULT_UNLOCK_INTERVAL)
    }
    println("cleaning done")
}
