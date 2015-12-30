package pqueue

import (
	"firempq/features"
	"firempq/log"
	"sort"
	"sync"
	"time"

	. "firempq/api"
	. "firempq/common"
	. "firempq/conf"
	. "firempq/features/pqueue/pqmsg"
	. "firempq/structs/pheap"
)

type PQueue struct {
	features.DBService
	// Payload access should be protected separately.
	payloadLock sync.Mutex
	// For messages jumping around all their jumps should be protected.
	lock sync.Mutex
	// Currently available messages to be popped.
	availMsgs *PHeap
	// All messages with the ticking counters except those which are inFlight.
	expireHeap *PHeap
	// All locked messages
	inFlightHeap *PHeap
	// Just a user defined id set.
	id2sn map[string]uint64
	// SerialNumber to message data.
	sn2msg map[uint64]*PQMsgMetaData

	// Set as True if the service is closed.
	closedState BoolFlag

	// Instance of the database.
	config *PQConfig

	// A must attribute of each service containing all essential service information generated upon creation.
	desc *ServiceDescription
	// Shorter version of service name to identify this service.
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
		id2sn:              make(map[string]uint64),
		sn2msg:             make(map[uint64]*PQMsgMetaData),
		availMsgs:          NewPHeap(),
		expireHeap:         NewPHeap(),
		inFlightHeap:       NewPHeap(),
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
	features.SaveServiceConfig(pq.GetServiceId(), pq.config)
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
	return len(pq.id2sn)
}

func (pq *PQueue) GetTypeName() string {
	return STYPE_PRIORITY_QUEUE
}

// Clear drops all locked and unlocked messages in the queue.
func (pq *PQueue) Clear() {
	total := 0
	for {
		snList := []uint64{}
		pq.lock.Lock()
		if len(pq.id2sn) == 0 {
			pq.lock.Unlock()
			break
		}
		for k, _ := range pq.sn2msg {
			snList = append(snList, k)
			if len(snList) > 100 {
				break
			}
		}
		total += len(snList)
		for _, sn := range snList {
			pq.deleteMessage(sn)
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
		case <-GetQuitChan():
			return NewItemsResponse(msgItems)
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
	sn, ok := pq.id2sn[msgId]
	if !ok {
		pq.lock.Unlock()
		return ERR_MSG_NOT_FOUND
	}
	locked := pq.inFlightHeap.ContainsIntId(sn)
	msg := pq.sn2msg[sn]
	if locked {
		unlockTs = msg.UnlockTs
	}
	data := map[string]interface{}{
		MSG_INFO_ID:        msgId,
		MSG_INFO_LOCKED:    locked,
		MSG_INFO_UNLOCK_TS: unlockTs,
		MSG_INFO_POP_COUNT: msg.PopCount,
		MSG_INFO_PRIORITY:  msg.Priority,
		MSG_INFO_EXPIRE_TS: msg.ExpireTs,
	}
	pq.lock.Unlock()
	return NewDictResponse(data)
}

func (pq *PQueue) DeleteLockedById(msgId string) IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	sn := pq.id2sn[msgId]

	if sn == 0 {
		return ERR_MSG_NOT_FOUND
	}

	if !pq.inFlightHeap.Remove(sn) {
		return ERR_MSG_NOT_LOCKED
	}

	pq.deleteMessage(sn)

	return OK_RESPONSE
}

func (pq *PQueue) DeleteById(msgId string) IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	sn := pq.id2sn[msgId]
	if sn == 0 {
		return ERR_MSG_NOT_FOUND
	}
	if pq.inFlightHeap.ContainsIntId(sn) {
		return ERR_MSG_IS_LOCKED
	}
	pq.deleteMessage(sn)
	return OK_RESPONSE
}

func (pq *PQueue) Push(msgId, payload string, msgTtl, delay, priority int64) IResponse {

	if int64(len(pq.id2sn)) >= pq.config.MaxSize {
		return ERR_SIZE_EXCEEDED
	}

	if priority >= pq.config.MaxPriority {
		return ERR_PRIORITY_OUT_OF_RANGE
	}

	nowTs := Uts()
	pq.config.LastPushTs = nowTs
	msg := NewPQMsgMetaData(msgId, priority, nowTs+msgTtl+delay, 0)

	pq.lock.Lock()

	if _, ok := pq.id2sn[msgId]; ok {
		pq.lock.Unlock()
		return ERR_ITEM_ALREADY_EXISTS
	}

	pq.msgSerialNumber++
	sn := pq.msgSerialNumber
	msg.SerialNumber = sn
	// Message should start expiring since the moment it was added into general pool of available messages.

	pq.id2sn[msgId] = sn
	pq.sn2msg[sn] = msg

	if delay == 0 {
		pq.expireHeap.Push(msg.ExpireTs, sn)
		pq.availMsgs.Push(msg.Priority, sn)
	} else {
		msg.UnlockTs = nowTs + delay
		pq.inFlightHeap.Push(msg.UnlockTs, sn)
	}
	// Payload is a race conditional case, since it is not always flushed on disk and may or may not exist in memory.
	pq.payloadLock.Lock()
	pq.StoreFullItemInDB(EncodeUint64ToString(sn), msg.StringMarshal(), payload)
	pq.payloadLock.Unlock()
	pq.lock.Unlock()

	NewMessageNotify(pq.newMsgNotification)

	return OK_RESPONSE
}

func (pq *PQueue) popMessages(lockTimeout int64, limit int64, lock bool) []IResponseItem {
	nowTs := Uts()
	pq.config.LastPopTs = nowTs
	var msgs []IResponseItem

	for int64(len(msgs)) < limit {

		pq.lock.Lock()
		if pq.availMsgs.Empty() {
			pq.lock.Unlock()
			return msgs
		}

		sn := pq.availMsgs.Pop().IntId
		msg, _ := pq.sn2msg[sn]

		msg.UnlockTs = nowTs + lockTimeout
		msg.PopCount += 1

		pq.expireHeap.Remove(sn)

		snDb := EncodeUint64ToString(sn)
		if lock {
			msg.Priority = -1
			pq.inFlightHeap.Push(msg.UnlockTs, sn)
			pq.StoreItemBodyInDB(snDb, msg.StringMarshal())
		} else {
			delete(pq.id2sn, msg.StrId)
			delete(pq.sn2msg, sn)
		}

		pq.payloadLock.Lock()
		pq.lock.Unlock()
		payload := pq.GetPayloadFromDB(snDb)
		msgs = append(msgs, NewMsgResponseItem(msg.StrId, payload, msg.ExpireTs, msg.PopCount, msg.UnlockTs))

		if !lock {
			pq.DeleteFullItemFromDB(snDb)
		}

		pq.payloadLock.Unlock()
	}
	return msgs
}

// UpdateLock sets a user defined message lock timeout.
// It works only for locked messages.
func (pq *PQueue) UpdateLock(msgId string, lockTimeout int64) IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	sn := pq.id2sn[msgId]
	if sn > 0 {
		if pq.inFlightHeap.Remove(sn) {
			msg := pq.sn2msg[sn]
			msg.UnlockTs = Uts() + lockTimeout
			pq.inFlightHeap.Push(msg.UnlockTs, sn)
			pq.StoreItemBodyInDB(msgId, msg.StringMarshal())
			return OK_RESPONSE
		} else {
			return ERR_MSG_NOT_LOCKED
		}
	} else {
		return ERR_MSG_NOT_FOUND
	}
}

func (pq *PQueue) UnlockMessageById(msgId string) IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	// Make sure message exists.
	sn := pq.id2sn[msgId]
	if sn == 0 {
		return ERR_MSG_NOT_FOUND
	}
	if !pq.inFlightHeap.Remove(sn) {
		return ERR_MSG_NOT_LOCKED
	}
	// Message exists, push it into the front of the queue.
	pq.returnToFront(pq.sn2msg[sn])
	return OK_RESPONSE
}

func (pq *PQueue) deleteMessage(sn uint64) bool {
	if msg, ok := pq.sn2msg[sn]; ok {
		delete(pq.id2sn, msg.StrId)
		delete(pq.sn2msg, sn)
		if pq.availMsgs.Remove(sn) {
			pq.expireHeap.Remove(sn)
		} else {
			pq.inFlightHeap.Remove(sn)
		}
		pq.payloadLock.Lock()
		pq.DeleteFullItemFromDB(EncodeUint64ToString(sn))
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
			pq.deleteMessage(msg.SerialNumber)
			return nil
		}
	}
	msg.UnlockTs = 0
	sn := msg.SerialNumber
	pq.availMsgs.Push(msg.Priority, sn)
	pq.expireHeap.Push(msg.ExpireTs, sn)
	pq.StoreItemBodyInDB(EncodeUint64ToString(sn), msg.StringMarshal())
	return nil
}

// Unlocks all items which exceeded their lock time.
func (pq *PQueue) releaseInFlight(ts int64) int64 {
	ifHeap := pq.inFlightHeap
	var counter int64 = 0
	for ifHeap.NotEmpty() && ifHeap.MinItem() < ts && counter < CFG_PQ.UnlockBatchSize {
		counter++
		sn := ifHeap.Pop().IntId
		msg := pq.sn2msg[sn]
		// Messages where popcount == 0 are messages with the delivery delay.
		if msg.PopCount == 0 {
			pq.expireHeap.Push(msg.ExpireTs, sn)
			pq.availMsgs.Push(msg.Priority, sn)
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
	for eh.NotEmpty() && eh.MinItem() < ts && counter < CFG_PQ.ExpirationBatchSize {
		counter++
		pq.deleteMessage(eh.Pop().IntId)
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
		return r1 >= CFG_PQ.UnlockBatchSize || r2 > CFG_PQ.ExpirationBatchSize
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
	delSn := []uint64{}

	cfg := pq.config
	for ; msgIter.Valid(); msgIter.Next() {
		sn := DecodeBytesToUnit64(msgIter.GetTrimKey())
		pqmsg := UnmarshalPQMsgMetaData(sn, msgIter.GetValue())
		// Message data has errors.
		if pqmsg == nil {
			continue
		}

		// Store list if message IDs that should be removed.
		if pqmsg.ExpireTs <= nowTs || (pqmsg.PopCount >= cfg.PopCountLimit && cfg.PopCountLimit > 0) {
			delSn = append(delSn, sn)
		} else {
			msgs = append(msgs, pqmsg)
		}
	}
	msgIter.Close()

	log.Debug("Loaded %d messages for %s queue", len(msgs), pq.desc.Name)
	if len(delSn) > 0 {
		log.Debug("Deleting %d expired messages", len(delSn))
		for _, dsn := range delSn {
			pq.DeleteFullItemFromDB(EncodeUint64ToString(dsn))
		}
	}
	// Sorting data guarantees that messages will be available in the same order as they arrived.
	sort.Sort(msgs)

	// Update serial number to match the latest message.
	if len(msgs) > 0 {
		pq.msgSerialNumber = msgs[len(msgs)-1].SerialNumber
	}

	for _, msg := range msgs {
		sn := msg.SerialNumber
		pq.id2sn[msg.StrId] = sn
		pq.sn2msg[sn] = msg
		if msg.UnlockTs > nowTs {
			pq.inFlightHeap.Push(msg.UnlockTs, sn)
		} else {
			pq.expireHeap.Push(msg.ExpireTs, sn)
			pq.availMsgs.Push(msg.Priority, sn)
		}
	}

	log.Debug("Messages available: %d", pq.expireHeap.Len())
	log.Debug("Messages are in flight: %d", pq.inFlightHeap.Len())

}

var _ ISvc = &PQueue{}
