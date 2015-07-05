package pqueue

import (
	"firempq/defs"
	"firempq/qerrors"
	"firempq/queue_facade"
	"firempq/queues/priority_first"
	"firempq/structs"
	"github.com/jmhodges/levigo"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	DEFAULT_TTL             = 12000000
	DEFAULT_DELIVERY_DELAY  = 0
	DEFAULT_LOCK_TIMEOUT    = 30000000
	DEFAULT_POP_COUNT_LIMIT = 0 // 0 means Unlimited.
)

const (
	ACTION_UNLOCK_BY_ID        = "UNLOCK"
	ACTION_DELETE_LOCKED_BY_ID = "DELLOCKED"
	ACTION_SET_LOCK_TIMEOUT    = "SETLOCKTIMEOUT"
)

type InMemCache struct {
	Msg      []byte
	Payload  string
	ToDelete bool
}

type PQueue struct {
	queueName string
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
	cacheLock      sync.Mutex
	workDone       bool
	actionHandlers map[string](func(map[string]string) error)
	msgDb          *levigo.DB
	payloadDb      *levigo.DB

	inMemMsgs map[string]InMemCache
}

func Uts() int64 {
	return time.Now().UnixNano() / 1000000
}

func NewPQueue(queueName string, maxPriority int64, maxSize int64) *PQueue {
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
		queueName:    queueName,
	}
	pq.inMemMsgs = make(map[string]InMemCache)

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

	var err error
	msgDbName := "pqueue_msgs_" + queueName + ".db"
	dbOpts := levigo.NewOptions()
	dbOpts.SetCreateIfMissing(true)
	dbOpts.SetWriteBufferSize(10 * 1024 * 1024)
	pq.msgDb, err = levigo.Open(msgDbName, dbOpts)
	if err != nil {
		log.Fatal("Can not open database: ", msgDbName, err)
	}

	payloadDbName := "pqueue_payload_" + queueName + ".db"
	pdbOpts := levigo.NewOptions()
	pdbOpts.SetWriteBufferSize(10 * 1024 * 1024)
	pdbOpts.SetCreateIfMissing(true)
	pq.payloadDb, err = levigo.Open(payloadDbName, pdbOpts)
	if err != nil {
		pq.msgDb.Close()
		log.Fatal("Can not open database: ", payloadDbName, err)
	}

	pq.loadAllMessages()

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
func (pq *PQueue) Push(msg *PQMessage, payload string) error {
	pq.LastPushTs = Uts()

	pq.lock.Lock()
	defer pq.lock.Unlock()
	return pq.storeMessage(msg, payload)
}

func (pq *PQueue) PushMessage(msgData map[string]string, payload string) error {
	msg, err := MessageFromMap(msgData)
	if err != nil {
		return err
	}
	return pq.Push(msg, payload)
}

func (pq *PQueue) storeMessage(msg *PQMessage, payload string) error {
	if _, ok := pq.allMessagesMap[msg.Id]; ok {
		return qerrors.ERR_ITEM_ALREADY_EXISTS
	}

	pq.trackExpiration(msg)
	pq.allMessagesMap[msg.Id] = msg
	pq.availableMsgs.Push(msg.Id, msg.Priority)

	pq.updateMessage(msg)
	pq.savePayload(msg.Id, payload)
	return nil
}

// Pop first available message.
// Will return nil if there are no messages available.
func (pq *PQueue) Pop() *PQMessage {

	pq.lock.Lock()
	defer pq.lock.Unlock()

	if pq.availableMsgs.Empty() {
		return nil
	}
	msgId := pq.availableMsgs.Pop()
	msg, ok := pq.allMessagesMap[msgId]

	if !ok {
		return nil
	}
	msg.PopCount += 1
	pq.lockMessage(msg)
	pq.updateMessage(msg)
	return msg
}

func (pq *PQueue) PopMessage() (queue_facade.IMessage, error) {
	msg := pq.Pop()
	if msg == nil {
		return nil, qerrors.ERR_QUEUE_EMPTY
	}
	return msg, nil
}

func (pq *PQueue) GetMessagePayload(msgId string) string {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	return pq.loadPayload(msgId)
}

func (pq *PQueue) lockMessage(msg *PQMessage) {
	nowTs := Uts()
	pq.LastPopTs = nowTs
	// Increase number of pop attempts.

	msg.UnlockTs = nowTs + pq.PopLockTimeout
	pq.expireHeap.PopById(msg.Id)
	pq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
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

	if !pq.deleteMessage(msgId) {
		return qerrors.ERR_MSG_NOT_EXIST
	}

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

	msg, err := pq.unflightMessage(msgId)
	if err != nil {
		return err
	}

	msg.UnlockTs = Uts() + int64(timeout)

	pq.inFlightHeap.PushItem(msgId, msg.UnlockTs)
	pq.updateMessage(msg)

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
	pq.deleteMessage(msgId)
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

func (pq *PQueue) deleteMessage(msgId string) bool {
	if _, ok := pq.allMessagesMap[msgId]; ok {
		delete(pq.allMessagesMap, msgId)
		pq.deleteDBMessage(msgId)
		return true
	}
	return false
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
			pq.deleteMessage(msg.Id)
			return qerrors.ERR_MSG_POP_ATTEMPTS_EXCEEDED
		}
	}
	msg.UnlockTs = 0
	pq.availableMsgs.PushFront(msg.Id)
	pq.trackExpiration(msg)
	pq.updateMessage(msg)
	return nil
}

// Unlocks all items which exceeded their lock time.
func (pq *PQueue) releaseInFlight() int {
	cur_ts := Uts()
	ifHeap := pq.inFlightHeap

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

	i := 0
	for !(eh.Empty()) && eh.MinElement() < cur_ts {
		i++
		hi := eh.PopItem()
		// There are two places to remove expired item:
		// 1. Map of all items - all items map.
		// 2. Available message.
		msg := pq.allMessagesMap[hi.Id]
		pq.availableMsgs.RemoveItem(msg.Id, msg.Priority)
		pq.deleteMessage(msg.Id)
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
const DEFAULT_UNLOCK_INTERVAL = 100 * 1000000 // 0.1 second

// Remove expired items. Should be running as a thread.
func (pq *PQueue) periodicCleanUp() {
	for !(pq.workDone) {
		var sleepTime time.Duration = DEFAULT_UNLOCK_INTERVAL
		pq.lock.Lock()
		cleaned := pq.releaseInFlight()
		pq.lock.Unlock()
		if cleaned > 0 {
			log.Println(cleaned, "messages returned to the front of the queue.")
			sleepTime = SLEEP_INTERVAL_IF_ITEMS
		}
		pq.lock.Lock()
		cleaned = pq.cleanExpiredItems()
		pq.lock.Unlock()
		if cleaned > 0 {
			log.Println(cleaned, "items expired.")
			sleepTime = SLEEP_INTERVAL_IF_ITEMS
		}
		pq.flushCache()
		time.Sleep(sleepTime)
	}
}

// Database related data management.
type MessageSlice []*PQMessage

func (p MessageSlice) Len() int           { return len(p) }
func (p MessageSlice) Less(i, j int) bool { return p[i].CreatedTs < p[j].CreatedTs }
func (p MessageSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (pq *PQueue) loadAllMessages() {
	nowTs := Uts()
	ropts := levigo.NewReadOptions()
	iter := pq.msgDb.NewIterator(ropts)
	iter.SeekToFirst()

	msgs := MessageSlice{}
	delIds := []string{}

	for iter.Valid() {
		pqmsg := PQMessageFromBinary(string(iter.Key()), iter.Value())

		// Store list if message IDs that should be removed.
		if pqmsg.CreatedTs+pq.MsgTTL < nowTs || (pqmsg.PopCount >= pq.PopCountLimit && pq.PopCountLimit > 0) {
			delIds = append(delIds, pqmsg.Id)
		} else {
			msgs = append(msgs, pqmsg)
		}
		iter.Next()
	}
	defer iter.Close()
	log.Printf("Loaded %d messages for %s queue", len(msgs), pq.queueName)
	if len(delIds) > 0 {
		log.Printf("%d messages will be removed because of expiration", len(delIds))
		for _, msgId := range delIds {
			pq.deleteDBMessage(msgId)
		}

	}
	// Sorting data guarantees that messages will be available almost in the same order as they arrived.
	sort.Sort(msgs)

	for _, msg := range msgs {
		pq.allMessagesMap[msg.Id] = msg
		if msg.UnlockTs > nowTs {
			pq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
		} else {
			pq.expireHeap.PushItem(msg.Id, msg.CreatedTs+pq.MsgTTL)
			pq.availableMsgs.Push(msg.Id, msg.Priority)
		}
	}

	log.Printf("Messages available: %d", pq.expireHeap.Len())
	log.Printf("Messages in flight: %d", pq.inFlightHeap.Len())
}

func (pq *PQueue) DeleteAll() {
	ropts := levigo.NewReadOptions()
	iter := pq.msgDb.NewIterator(ropts)
	defer iter.Close()
	for iter.Valid() {
		id := string(iter.Key())
		pq.DeleteLockedById(map[string]string{defs.PARAM_MSG_ID: id})
		pq.DeleteById(id)
		iter.Next()
	}
}

func (pq *PQueue) flushCache() {
	if len(pq.inMemMsgs) == 0 {
		return
	}

	pq.lock.Lock()
	currCache := pq.inMemMsgs
	pq.inMemMsgs = make(map[string]InMemCache)

	msgWb := levigo.NewWriteBatch()
	payloadWb := levigo.NewWriteBatch()

	for k, v := range currCache {
		if v.ToDelete {
			id := []byte(k)
			msgWb.Delete(id)
			payloadWb.Delete(id)
			continue

		}
		_, exists := pq.allMessagesMap[k]
		if exists {
			id := []byte(k)
			if len(v.Msg) > 0 {
				msgWb.Put(id, v.Msg)
			}
			if len(v.Payload) > 0 {
				payloadWb.Put(id, []byte(v.Payload))
			}
		}
	}
	pq.lock.Unlock()
	pq.cacheLock.Lock()
	defer pq.cacheLock.Unlock()

	wopts := levigo.NewWriteOptions()
	pq.msgDb.Write(wopts, msgWb)
	pq.payloadDb.Write(wopts, payloadWb)

}

func (pq *PQueue) updateMessage(msg *PQMessage) {
	data, ok := pq.inMemMsgs[msg.Id]
	if !ok {
		cacheMsg := InMemCache{msg.ToBinary(), "", false}
		pq.inMemMsgs[msg.Id] = cacheMsg
	} else {
		data.Msg = msg.ToBinary()
	}
}

func (pq *PQueue) savePayload(msgId, payload string) {
	data, ok := pq.inMemMsgs[msgId]
	if ok {
		pq.inMemMsgs[msgId] = InMemCache{data.Msg, payload, false}
	} else {
		pq.inMemMsgs[msgId] = InMemCache{nil, payload, false}
	}
}

func (pq *PQueue) deleteDBMessage(msgId string) {
	pq.inMemMsgs[msgId] = InMemCache{nil, "", true}
}

func (pq *PQueue) loadPayload(msgId string) string {

	cacheData, ok := pq.inMemMsgs[msgId]
	if ok && len(cacheData.Payload) > 0 {
		return cacheData.Payload
	}

	ropts := levigo.NewReadOptions()
	value, err := pq.payloadDb.Get(ropts, []byte(msgId))
	if err != nil {
		return ""
	}
	return string(value)
}

func (pq *PQueue) Close() {
	pq.lock.Lock()
	if pq.workDone {
		return
	}
	pq.workDone = true
	pq.lock.Unlock()

	pq.flushCache()

	pq.msgDb.Close()
	pq.payloadDb.Close()

}

var _ queue_facade.IQueue = &PQueue{}
