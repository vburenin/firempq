package pqueue

import (
	"firempq/features"
	"firempq/log"
	"firempq/structs"
	"sort"
	"sync"
	"time"

	. "firempq/api"
	. "firempq/common"
	. "firempq/conf"
)

type PQueue struct {
	features.DBService
	// Currently available messages to be popped.
	availMsgs *structs.PriorityFirstQueue
	// All messages with the ticking counters except those which are inFlight.
	expireHeap *structs.IndexedPriorityQueue
	// All locked messages
	inFlightHeap *structs.IndexedPriorityQueue
	// Just a message map message id to the full message data.
	msgMap map[string]*PQMsgMetaData

	// Set as True if the service is closed.
	closedState BoolFlag

	// Instance of the database.
	config *PQConfig

	// A must attribute of each service containing all essential service information generated upon creation.
	desc *ServiceDescription
	// Shorter version of service name to identify this service.
	lock               sync.Mutex
	payloadLock        sync.Mutex
	newMsgNotification chan struct{}

	msgSerialNumber uint64
}

func CreatePQueue(desc *ServiceDescription, params []string) ISvc {
	return NewPQueue(desc, 100, 1000)
}

func initPQueue(desc *ServiceDescription, config *PQConfig) *PQueue {
	pq := PQueue{
		desc:               desc,
		config:             config,
		msgMap:             make(map[string]*PQMsgMetaData),
		availMsgs:          structs.NewActiveQueues(config.MaxPriority),
		expireHeap:         structs.NewIndexedPriorityQueue(),
		inFlightHeap:       structs.NewIndexedPriorityQueue(),
		newMsgNotification: make(chan struct{}),
		msgSerialNumber:    0,
	}
	// Init inherited service db.
	pq.InitServiceDB(desc.ServiceId)
	pq.loadAllMessages()
	return &pq
}

func NewPQueue(desc *ServiceDescription, priorities int64, size int64) *PQueue {
	config := &PQConfig{
		MaxPriority:    priorities,
		MaxSize:        size,
		MsgTtl:         CFG_PQ.DefaultMessageTtl,
		DeliveryDelay:  CFG_PQ.DefaultDeliveryDelay,
		PopLockTimeout: CFG_PQ.DefaultLockTimeout,
		PopCountLimit:  CFG_PQ.DefaultPopCountLimit,
		LastPushTs:     Uts(),
		LastPopTs:      Uts(),
		InactivityTtl:  0,
	}

	queue := initPQueue(desc, config)
	features.SaveServiceConfig(desc.ServiceId, config)
	return queue
}

func LoadPQueue(desc *ServiceDescription) (ISvc, error) {
	config := &PQConfig{}
	err := features.LoadServiceConfig(desc.ServiceId, config)
	if err != nil {
		return nil, err
	}
	pq := initPQueue(desc, config)
	return pq, nil
}

func (pq *PQueue) NewContext(rw ResponseWriter) ServiceContext {
	return NewPQContext(pq, rw)
}

const (
	PQ_STATUS_MAX_PRIORITY     = "MaxPriority"
	PQ_STATUS_MAX_SIZE         = "MaxSize"
	PQ_STATUS_MSG_TTL          = "MsgTtl"
	PQ_STATUS_DELIVERY_DELAY   = "DeliveryDelay"
	PQ_STATUS_POP_LOCK_TIMEOUT = "PopLockTimeout"
	PQ_STATUS_POP_COUNT_LIMIT  = "PopCountLimit"
	PQ_STATUS_CREATE_TS        = "CreateTs"
	PQ_STATUS_LAST_PUSH_TS     = "LastPushTs"
	PQ_STATUS_LAST_POP_TS      = "LastPopTs"
	PQ_STATUS_INACTIVITY_TTL   = "InactivityTtl"
	PQ_STATUS_TOTAL_MSGS       = "TotalMessages"
	PQ_STATUS_IN_FLIGHT_MSG    = "InFlightMessages"
	PQ_STATUS_AVAILABLE_MSGS   = "AvailableMessages"
)

func (pq *PQueue) GetStatus() map[string]interface{} {
	res := make(map[string]interface{})
	res[PQ_STATUS_MAX_PRIORITY] = pq.config.GetMaxPriority()
	res[PQ_STATUS_MAX_SIZE] = pq.config.GetMaxSize()
	res[PQ_STATUS_MSG_TTL] = pq.config.GetMsgTtl()
	res[PQ_STATUS_DELIVERY_DELAY] = pq.config.GetDeliveryDelay()
	res[PQ_STATUS_POP_LOCK_TIMEOUT] = pq.config.GetPopLockTimeout()
	res[PQ_STATUS_POP_COUNT_LIMIT] = pq.config.GetPopCountLimit()
	res[PQ_STATUS_CREATE_TS] = pq.desc.GetCreateTs()
	res[PQ_STATUS_LAST_PUSH_TS] = pq.config.GetLastPushTs()
	res[PQ_STATUS_LAST_POP_TS] = pq.config.GetLastPopTs()
	res[PQ_STATUS_INACTIVITY_TTL] = pq.config.GetInactivityTtl()
	res[PQ_STATUS_TOTAL_MSGS] = pq.GetSize()
	res[PQ_STATUS_IN_FLIGHT_MSG] = pq.inFlightHeap.Len()
	res[PQ_STATUS_AVAILABLE_MSGS] = pq.GetSize() - pq.inFlightHeap.Len()
	return res
}

func (pq *PQueue) SetParams(msgTtl, maxSize, queueTtl, deliveryDelay int64) IResponse {
	pq.lock.Lock()
	pq.config.MsgTtl = msgTtl
	pq.config.MaxSize = maxSize
	pq.config.InactivityTtl = queueTtl
	pq.config.DeliveryDelay = deliveryDelay
	features.SaveServiceConfig(pq.desc.ServiceId, pq.config)
	pq.lock.Unlock()
	return OK_RESPONSE
}

func (pq *PQueue) GetCurrentStatus() IResponse {
	return NewDictResponse(pq.GetStatus())
}

func (pq *PQueue) GetServiceId() string {
	return pq.desc.ServiceId
}

func (pq *PQueue) GetSize() int {
	return len(pq.msgMap)
}

func (pq *PQueue) GetTypeName() string {
	return STYPE_PRIORITY_QUEUE
}

// Clear drops all locked and unlocked messages in the queue.
func (pq *PQueue) Clear() {
	total := 0
	for {
		ids := []string{}
		pq.lock.Lock()
		if len(pq.msgMap) == 0 {
			pq.lock.Unlock()
			break
		}
		for k, _ := range pq.msgMap {
			ids = append(ids, k)
			if len(ids) > 100 {
				break
			}
		}
		total += len(ids)
		for _, id := range ids {
			pq.deleteMessage(id)
		}
		pq.lock.Unlock()
	}
	log.Debug("Removed %d messages.", total)
}

func (pq *PQueue) Close() {
	log.Debug("Closing PQueue service: %s", pq.desc.Name)
	pq.closedState.SetTrue()
}

func (pq *PQueue) IsClosed() bool {
	return pq.closedState.IsTrue()
}

func (pq *PQueue) ExpireItems(cutOffTs int64) IResponse {
	var total int64
	pq.lock.Lock()

	for value := pq.cleanExpiredItems(cutOffTs); value > 0; value = pq.cleanExpiredItems(cutOffTs) {
		total += value
	}

	pq.lock.Unlock()

	return NewIntResponse(total)
}

func (pq *PQueue) ReleaseInFlight(cutOffTs int64) IResponse {
	var total int64
	pq.lock.Lock()

	for value := pq.releaseInFlight(cutOffTs); value > 0; value = pq.releaseInFlight(cutOffTs) {
		total += value
	}

	pq.lock.Unlock()

	return NewIntResponse(total)
}

// PopWaitItems pops 'limit' messages within 'timeout'(milliseconds) time interval.
func (pq *PQueue) Pop(lockTimeout, popWaitTimeout, limit int64, lock bool) IResponse {
	// Try to pop items first time and return them if number of popped items is greater than 0.
	msgItems := pq.popMessages(lockTimeout, limit, lock)
	if len(msgItems) > 0 || popWaitTimeout == 0 {
		return NewItemsResponse(msgItems)
	}

	for {
		select {
		case <-pq.newMsgNotification:
			msgItems := pq.popMessages(lockTimeout, limit, lock)
			if len(msgItems) > 0 {
				return NewItemsResponse(msgItems)
			}
		case <-time.After(time.Duration(popWaitTimeout) * time.Millisecond):
			return NewItemsResponse(pq.popMessages(lockTimeout, limit, lock))
		}
	}
}

const (
	MSG_INFO_ID        = "Id"
	MSG_INFO_LOCKED    = "Locked"
	MSG_INFO_UNLOCK_TS = "UnlockTs"
	MSG_INFO_POP_COUNT = "PopCount"
	MSG_INFO_PRIORITY  = "Priority"
	MSG_INFO_EXPIRE_TS = "ExpireTs"
)

func (pq *PQueue) GetMessageInfo(msgId string) IResponse {
	var unlockTs int64
	pq.lock.Lock()
	msgInfo, ok := pq.msgMap[msgId]

	if !ok {
		pq.lock.Unlock()
		return ERR_MSG_NOT_FOUND
	}
	locked := pq.inFlightHeap.ContainsId(msgId)
	if locked {
		unlockTs = msgInfo.UnlockTs
	}
	data := map[string]interface{}{
		MSG_INFO_ID:        msgId,
		MSG_INFO_LOCKED:    locked,
		MSG_INFO_UNLOCK_TS: unlockTs,
		MSG_INFO_POP_COUNT: msgInfo.PopCount,
		MSG_INFO_PRIORITY:  msgInfo.Priority,
		MSG_INFO_EXPIRE_TS: msgInfo.ExpireTs,
	}
	pq.lock.Unlock()
	return NewDictResponse(data)
}

func (pq *PQueue) DeleteLockedById(msgId string) IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	_, err := pq.unflightMessage(msgId)
	if err != nil {
		return err
	}
	pq.deleteMessage(msgId)
	return OK_RESPONSE
}

func (pq *PQueue) DeleteById(msgId string) IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	if pq.inFlightHeap.ContainsId(msgId) {
		return ERR_MSG_IS_LOCKED
	}
	if !pq.deleteMessage(msgId) {
		return ERR_MSG_NOT_FOUND
	}

	return OK_RESPONSE
}

func (pq *PQueue) Push(msgId, payload string, msgTtl, delay, priority int64) IResponse {

	if int64(len(pq.msgMap)) >= pq.config.MaxSize {
		return ERR_SIZE_EXCEEDED
	}

	if priority >= pq.config.MaxPriority {
		return ERR_PRIORITY_OUT_OF_RANGE
	}

	if len(msgId) == 0 {
		msgId = GenRandMsgId()
	}

	nowTs := Uts()
	pq.config.LastPushTs = nowTs

	pq.lock.Lock()

	if _, ok := pq.msgMap[msgId]; ok {
		pq.lock.Unlock()
		return ERR_ITEM_ALREADY_EXISTS
	}

	pq.msgSerialNumber++
	msg := NewPQMsgMetaData(msgId, priority, nowTs+msgTtl, pq.msgSerialNumber)

	pq.msgMap[msg.Id] = msg
	if delay == 0 {
		pq.expireHeap.PushItem(msg.Id, msg.ExpireTs)
		pq.availMsgs.Push(msgId, msg.Priority)
	} else {
		msg.UnlockTs = nowTs + delay
		pq.inFlightHeap.PushItem(msgId, msg.UnlockTs)
	}
	// Payload is a race conditional case, since it is not always flushed on disk and may or may not exist in memory.
	pq.payloadLock.Lock()
	pq.StoreFullItemInDB(msg, payload)
	pq.payloadLock.Unlock()

	pq.lock.Unlock()

	NewMessageNotify(pq.newMsgNotification)

	return OK_RESPONSE
}

func (pq *PQueue) popMessages(lockTimeout int64, limit int64, lock bool) []IItem {
	nowTs := Uts()
	pq.config.LastPopTs = nowTs
	var msgs []IItem

	for int64(len(msgs)) < limit {
		pq.lock.Lock()
		msgId := pq.availMsgs.Pop()
		msg, ok := pq.msgMap[msgId]

		if !ok {
			pq.lock.Unlock()
			break
		}

		msg.UnlockTs = nowTs + lockTimeout
		msg.PopCount += 1
		pq.expireHeap.PopById(msg.Id)

		if lock {
			pq.inFlightHeap.PushItem(msgId, msg.UnlockTs)
			pq.StoreItemBodyInDB(msg)
		} else {
			delete(pq.msgMap, msgId)
		}

		pq.payloadLock.Lock()
		pq.lock.Unlock()
		msgs = append(msgs, NewMsgPayloadData(msgId, pq.GetPayloadFromDB(msgId)))

		if !lock {
			pq.DeleteFullItemFromDB(msgId)
		}

		pq.payloadLock.Unlock()
	}
	return msgs
}

// Remove message id from In Flight message heap.
func (pq *PQueue) unflightMessage(msgId string) (*PQMsgMetaData, *ErrorResponse) {
	msg, ok := pq.msgMap[msgId]
	if !ok {
		return nil, ERR_MSG_NOT_FOUND
	}

	hi := pq.inFlightHeap.PopById(msgId)
	if hi == structs.EmptyHeapItem {
		return nil, ERR_MSG_NOT_LOCKED
	}

	return msg, nil
}

// UpdateLock sets a user defined message lock timeout.
// It works only for locked messages.
func (pq *PQueue) UpdateLock(msgId string, lockTimeout int64) IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	msg, err := pq.unflightMessage(msgId)
	if err != nil {
		return err
	}

	msg.UnlockTs = Uts() + lockTimeout

	pq.inFlightHeap.PushItem(msgId, msg.UnlockTs)
	pq.StoreItemBodyInDB(msg)
	return OK_RESPONSE
}

func (pq *PQueue) UnlockMessageById(msgId string) IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	// Make sure message exists.
	msg, err := pq.unflightMessage(msgId)
	if err != nil {
		return err
	}
	// Message exists, push it into the front of the queue.
	err = pq.returnToFront(msg)
	if err != nil {
		return err
	}
	return OK_RESPONSE
}

func (pq *PQueue) deleteMessage(msgId string) bool {
	if msg, ok := pq.msgMap[msgId]; ok {
		delete(pq.msgMap, msgId)
		pq.availMsgs.RemoveItem(msgId, msg.Priority)
		pq.expireHeap.PopById(msgId)
		pq.payloadLock.Lock()
		pq.DeleteFullItemFromDB(msgId)
		pq.payloadLock.Unlock()
		return true
	}
	return false
}

// Attempts to return a message into the front of the queue.
// If a number of POP attempts has exceeded, message will be deleted.
func (pq *PQueue) returnToFront(msg *PQMsgMetaData) *ErrorResponse {
	lim := pq.config.PopCountLimit
	if lim > 0 {
		if lim <= msg.PopCount {
			pq.deleteMessage(msg.Id)
			return nil
		}
	}
	msg.UnlockTs = 0
	pq.availMsgs.PushFront(msg.Id)
	pq.expireHeap.PushItem(msg.Id, msg.ExpireTs)
	pq.StoreItemBodyInDB(msg)
	return nil
}

// Unlocks all items which exceeded their lock time.
func (pq *PQueue) releaseInFlight(ts int64) int64 {
	ifHeap := pq.inFlightHeap
	var counter int64 = 0
	for !(ifHeap.Empty()) && ifHeap.MinElement() < ts && counter < CFG_PQ.UnlockBatchSize {
		counter++
		unlockedItem := ifHeap.PopItem()
		msg := pq.msgMap[unlockedItem.Id]
		// Messages with popcount == 0 are messages with the delivery delay.
		if msg.PopCount == 0 {
			pq.expireHeap.PushItem(msg.Id, msg.ExpireTs)
			pq.availMsgs.Push(msg.Id, msg.Priority)
		} else {
			pq.returnToFront(msg)
		}
		NewMessageNotify(pq.newMsgNotification)
	}
	if counter > 0 {
		log.Debug("%d item(s) moved to the queue.", counter)
	}
	return counter
}

// Remove all items which are completely expired.
func (pq *PQueue) cleanExpiredItems(ts int64) int64 {
	var counter int64 = 0
	eh := pq.expireHeap
	for !(eh.Empty()) && eh.MinElement() < ts && counter < CFG_PQ.ExpirationBatchSize {
		counter++
		pq.deleteMessage(eh.PopItem().Id)
	}
	if counter > 0 {
		log.Debug("%d item(s) expired.", counter)
	}
	return counter
}

// StartUpdate runs a loop of periodic data updates.
func (pq *PQueue) StartUpdate() {
	go func() {
		for pq.closedState.IsFalse() {
			if pq.update(Uts()) {
				time.Sleep(time.Millisecond)
			} else {
				time.Sleep(CFG.UpdateInterval * time.Millisecond)
			}
		}
	}()
}

// Remove expired and return unlocked items. Should be running as a thread.
func (pq *PQueue) update(ts int64) bool {
	pq.closedState.Lock()
	defer pq.closedState.Unlock()
	if pq.closedState.IsFalse() {
		pq.lock.Lock()
		r1 := pq.releaseInFlight(ts)
		r2 := pq.cleanExpiredItems(ts)
		pq.lock.Unlock()
		return r1 > 0 || r2 > 0
	}
	return false
}

// MessageSlice data type to sort messages.
type MessageSlice []*PQMsgMetaData

func (p MessageSlice) Len() int           { return len(p) }
func (p MessageSlice) Less(i, j int) bool { return p[i].SerialNumber < p[j].SerialNumber }
func (p MessageSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (pq *PQueue) loadAllMessages() {
	nowTs := Uts()
	log.Debug("Initializing queue: %s", pq.desc.Name)
	msgIter := pq.GetItemIterator()
	msgs := MessageSlice{}
	delIds := []string{}

	cfg := pq.config
	for ; msgIter.Valid(); msgIter.Next() {
		msgId := UnsafeBytesToString(msgIter.GetTrimKey())
		pqmsg := UnmarshalPQMsgMetaData(msgId, msgIter.GetValue())

		// Message data has errors.
		if pqmsg == nil {
			continue
		}

		// Store list if message IDs that should be removed.
		if pqmsg.ExpireTs <= nowTs || (pqmsg.PopCount >= cfg.PopCountLimit && cfg.PopCountLimit > 0) {
			delIds = append(delIds, pqmsg.Id)
		} else {
			msgs = append(msgs, pqmsg)
		}
	}
	msgIter.Close()

	log.Debug("Loaded %d messages for %s queue", len(msgs), pq.desc.Name)
	if len(delIds) > 0 {
		log.Debug("Deleting %d expired messages", len(delIds))
		for _, msgId := range delIds {
			pq.DeleteFullItemFromDB(msgId)
		}
	}
	// Sorting data guarantees that messages will be available in the same order as they arrived.
	sort.Sort(msgs)

	// Update serial number to match the latest message.
	if len(msgs) > 0 {
		pq.msgSerialNumber = msgs[len(msgs)-1].SerialNumber
	}

	for _, msg := range msgs {
		pq.msgMap[msg.Id] = msg
		if msg.UnlockTs > nowTs {
			pq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
		} else {
			pq.expireHeap.PushItem(msg.Id, msg.ExpireTs)
			if msg.PopCount > 0 {
				pq.availMsgs.PushFront(msg.Id)
			} else {
				pq.availMsgs.Push(msg.Id, msg.Priority)
			}
		}
	}

	log.Debug("Messages available: %d", pq.expireHeap.Len())
	log.Debug("Messages are in flight: %d", pq.inFlightHeap.Len())

}

var _ ISvc = &PQueue{}
