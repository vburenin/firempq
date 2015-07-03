package pqueue

import (
	"firempq/defs"
	"firempq/qerrors"
	"firempq/queues/priority_first"
	"firempq/structs"
	"log"
	"strconv"
	"sync"
	"time"
)

const (
	DEFAULT_TTL             = 3600 * 24 * 14
	DEFAULT_DELIVERY_DELAY  = 0
	DEFAULT_LOCK_TIMEOUT    = 300000
	DEFAULT_POP_COUNT_LIMIT = 0 // Unlimited.
)

const (
	ACTION_UNLOCK_BY_ID        = "UNLOCK"
	ACTION_DELETE_LOCKED_BY_ID = "DELLOCKED"
	ACTION_SET_LOCK_TIMEOUT    = "SETLOCKTIMEOUT"
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

	MsgTTL         int64
	DeliveryDelay  int64
	PopLockTimeout int64
	PopCountLimit  int64

	MaxSize        int64 // Max queue size.
	CreateTs       int64 // Time when queue was created.
	LastPushTs     int64 // Last time item has been pushed into queue.
	LastPopTs      int64 // Last pop time.
	MaxPriority    int64
	lock           sync.Mutex
	workDone       bool
	actionHandlers map[string](func(map[string]string) error)
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
		LastPushTs:   0,
		LastPopTs:    0,
		inFlightHeap: structs.NewIndexHeap(),
		expireHeap:   structs.NewIndexHeap(),
		workDone:     false,
	}

	pq.actionHandlers = make(map[string](func(map[string]string) error))
	pq.actionHandlers[ACTION_UNLOCK_BY_ID] = pq.UnlockMessageById
	pq.actionHandlers[ACTION_DELETE_LOCKED_BY_ID] = pq.DeleteLockedById
	pq.actionHandlers[ACTION_SET_LOCK_TIMEOUT] = pq.SetLockTimeout

	pq.MsgTTL = DEFAULT_TTL
	pq.DeliveryDelay = DEFAULT_DELIVERY_DELAY
	pq.PopLockTimeout = DEFAULT_LOCK_TIMEOUT
	pq.PopCountLimit = DEFAULT_POP_COUNT_LIMIT

	pq.availableMsgs = priority_first.NewActiveQueues(maxPriority)
	pq.allMessagesMap = make(map[string]*PQMessage)
	go pq.periodicCleanUp()
	return &pq
}

func (pq *PQueue) GetStatus() map[string]interface{} {
	res := make(map[string]interface{})
	res["MaxPriority"] = pq.MaxPriority
	res["MaxSize"] = pq.MaxSize
	res["CreateTs"] = pq.CreateTs
	res["LastPushTs"] = pq.LastPushTs
	res["LastPopTs"] = pq.LastPopTs
	res["InFlightSize"] = pq.inFlightHeap.Len()
	res["TotalMessages"] = len(pq.allMessagesMap)
	res["PopTimeOut"] = pq.PopLockTimeout
	res["DeliveryDelay"] = pq.DeliveryDelay
	res["PopCountLimit"] = pq.PopCountLimit
	return res
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (pq *PQueue) Push(msg *PQMessage) error {
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

func (pq *PQueue) PushMessage(msgData map[string]string) error {
	msg, err := MessageFromMap(msgData)
	if err != nil {
		return err
	}
	return pq.Push(msg)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (pq *PQueue) Pop() *PQMessage {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	pq.LastPopTs = Uts()

	if pq.availableMsgs.Empty() {
		return nil
	}
	msgId := pq.availableMsgs.Pop()
	pqMsg, ok := pq.allMessagesMap[msgId]

	if !ok {
		return nil
	}

	// Increase number of pop attempts.
	pqMsg.PopCount += 1

	pq.expireHeap.PopById(msgId)
	pq.inFlightHeap.PushItem(msgId, Uts()+pq.PopLockTimeout)

	return pqMsg
}

// Remove message id from In Flight message heap.
func (pq *PQueue) unflightMessage(msgId string) (*PQMessage, error) {
	msg, ok := pq.allMessagesMap[msgId]
	if !ok {
		return nil, qerrors.ERR_MSG_NOT_EXIST
	}

	hi := pq.inFlightHeap.PopById(msgId)
	if hi == structs.EMPTY_HEAP_ITEM {
		return nil, qerrors.ERR_MSG_NOT_LOCKED
	}

	return msg, nil
}

func (pq *PQueue) DeleteById(msgId string) error {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	if pq.inFlightHeap.ContainsId(msgId) {
		return qerrors.ERR_MSG_IS_LOCKED
	}

	_, ok := pq.allMessagesMap[msgId]
	if !ok {
		return qerrors.ERR_MSG_NOT_EXIST
	}

	delete(pq.allMessagesMap, msgId)
	return nil
}

// Queue custom specific handler for the queue type specific features.
func (pq *PQueue) CustomHandler(action string, params map[string]string) error {
	handler, ok := pq.actionHandlers[action]
	if !ok {
		return qerrors.InvalidRequest("Unknown action: " + action)
	}
	return handler(params)
}

// Set a user defined message lock timeout. Only locked message timeout can be set.
func (pq *PQueue) SetLockTimeout(params map[string]string) error {
	msgId, ok := params[defs.PARAM_MSG_ID]
	if !ok {
		return qerrors.ERR_MSG_NOT_DEFINED
	}

	timeoutStr, ok := params[defs.PARAM_MSG_TIMEOUT]

	if !ok {
		return qerrors.ERR_MSG_TIMEOUT_NOT_DEFINED
	}

	timeout, terr := strconv.Atoi(timeoutStr)
	if terr != nil || timeout < 0 || timeout > defs.TIMEOUT_MAX_LOCK {
		return qerrors.ERR_MSG_TIMEOUT_IS_WRONG
	}

	pq.lock.Lock()
	defer pq.lock.Unlock()

	_, err := pq.unflightMessage(msgId)
	if err != nil {
		return err
	}
	pq.inFlightHeap.PushItem(msgId, Uts()+int64(timeout))
	return nil
}

// Delete locked message by id.
func (pq *PQueue) DeleteLockedById(params map[string]string) error {
	msgId, ok := params[defs.PARAM_MSG_ID]
	if !ok {
		return qerrors.ERR_MSG_NOT_DEFINED
	}

	pq.lock.Lock()
	defer pq.lock.Unlock()

	_, err := pq.unflightMessage(msgId)
	if err != nil {
		return err
	}
	delete(pq.allMessagesMap, msgId)
	return nil
}

func (pq *PQueue) UnlockMessageById(params map[string]string) error {

	msgId, ok := params[defs.PARAM_MSG_ID]
	if !ok {
		return qerrors.ERR_MSG_NOT_DEFINED
	}

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
// If a number of POP attempts has exceeded, message will be deleted.
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
		if i >= MAX_CLEANS_PER_ATTEMPT {
			break
		}
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
		if i >= MAX_CLEANS_PER_ATTEMPT {
			break
		}
	}
	return i
}

// 1 milliseconds.
const SLEEP_INTERVAL_IF_ITEMS = 1000000

// 1000 items should be enough to not create long locks. In the most good cases clean ups are rare.
const MAX_CLEANS_PER_ATTEMPT = 1000

// How frequently loop should run.
const DEFAULT_UNLOCK_INTERVAL = 100 * 1000000 // 1 second

// Remove expired items. Should be running as a thread.
func (pq *PQueue) periodicCleanUp() {
	for !(pq.workDone) {
		var sleepTime time.Duration = DEFAULT_UNLOCK_INTERVAL
		if cleaned := pq.releaseInFlight(); cleaned > 0 {
			log.Println(cleaned, "messages returned to the front of the queue.")
			sleepTime = SLEEP_INTERVAL_IF_ITEMS
		}
		if cleaned := pq.cleanExpiredItems(); cleaned > 0 {
			log.Println(cleaned, "items expired.")
			sleepTime = SLEEP_INTERVAL_IF_ITEMS
		}
		time.Sleep(sleepTime)
	}
}
