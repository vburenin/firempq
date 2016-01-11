package pqueue

import (
	"firempq/db"
	"firempq/log"
	"strings"
	"sync"
	"time"

	. "firempq/api"
	. "firempq/common"
	. "firempq/conf"
	. "firempq/encoding"
	. "firempq/errors"
	. "firempq/parsers"
	. "firempq/response"
	. "firempq/services/pqueue/pqmsg"
	. "firempq/services/svcmetadata"
	. "firempq/utils"
)

type PQueue struct {
	// A wrapper on top of common database operations.
	db.DBService
	// Payload access should be protected separately.
	payloadLock sync.Mutex
	// For messages jumping around all their jumps should be protected.
	lock sync.Mutex
	// Currently available messages to be popped.
	availMsgs *MsgHeap
	// All messages with the ticking counters except those which are inFlight.
	trackHeap *MsgHeap
	// All locked messages
	id2sn map[string]uint64
	// Set as True if the service is closed.
	closed BoolFlag
	// Instance of the database.
	config *PQConfig

	svcs IServices

	// This queue will be used to push messages exceeded pop limit attempts. All errors are ignored.
	popLimitMoveChan chan *PQMsgMetaData

	// A must attribute of each service containing all essential service information generated upon creation.
	desc *ServiceDescription
	// Shorter version of service name to identify this service.
	newMsgNotification chan struct{}

	// Serial number assigned to new messages.
	msgSerialNumber uint64

	// Number of message which are locked
	lockedMsgCnt int
}

func InitPQueue(svcs IServices, desc *ServiceDescription, config *PQConfig) *PQueue {
	pq := PQueue{
		desc:               desc,
		config:             config,
		svcs:               svcs,
		id2sn:              make(map[string]uint64),
		availMsgs:          NewSnHeap(),
		trackHeap:          NewTsHeap(),
		newMsgNotification: make(chan struct{}),
		msgSerialNumber:    0,
		lockedMsgCnt:       0,
		popLimitMoveChan:   make(chan *PQMsgMetaData, 16384),
	}
	// Init inherited service db.
	pq.InitServiceDB(desc.ServiceId)
	SaveServiceConfig(desc.ServiceId, config)
	pq.loadAllMessages()
	return &pq
}

func LoadPQueue(svcs IServices, desc *ServiceDescription) (ISvc, error) {
	config := &PQConfig{}
	err := LoadServiceConfig(desc.ServiceId, config)
	if err != nil {
		return nil, err
	}
	pq := InitPQueue(svcs, desc, config)
	return pq, nil
}

func (pq *PQueue) NewContext(rw ResponseWriter) ServiceContext {
	return NewPQContext(pq, rw)
}

// StartUpdate runs a loop of periodic data updates.
func (pq *PQueue) StartUpdate() {
	// Run a goroutine to move failed pops.
	go pq.moveToPopLimitedQueue()
	// Run timeout/unlock goroutine.
	go func() {
		var cnt int64
		for {
			pq.closed.Lock()
			if pq.closed.IsFalse() {
				pq.lock.Lock()
				cnt = pq.checkTimeouts(Uts())
				pq.lock.Unlock()
			} else {
				pq.closed.Unlock()
				break
			}
			pq.closed.Unlock()
			if cnt >= CFG_PQ.TimeoutCheckBatchSize {
				time.Sleep(time.Millisecond)
			} else {
				time.Sleep(CFG.UpdateInterval * time.Millisecond)
			}
		}
	}()
}

const (
	PQ_STATUS_MAX_SIZE         = "MaxSize"
	PQ_STATUS_MSG_TTL          = "MsgTtl"
	PQ_STATUS_DELIVERY_DELAY   = "DeliveryDelay"
	PQ_STATUS_POP_LOCK_TIMEOUT = "PopLockTimeout"
	PQ_STATUS_POP_COUNT_LIMIT  = "PopCountLimit"
	PQ_STATUS_CREATE_TS        = "CreateTs"
	PQ_STATUS_LAST_PUSH_TS     = "LastPushTs"
	PQ_STATUS_LAST_POP_TS      = "LastPopTs"
	PQ_STATUS_TOTAL_MSGS       = "TotalMessages"
	PQ_STATUS_IN_FLIGHT_MSG    = "InFlightMessages"
	PQ_STATUS_AVAILABLE_MSGS   = "AvailableMessages"
	PQ_STATUS_FAIL_QUEUE       = "FailQueue"
)

func (pq *PQueue) GetStatus() map[string]interface{} {
	res := make(map[string]interface{})
	res[PQ_STATUS_MAX_SIZE] = pq.config.MaxSize
	res[PQ_STATUS_MSG_TTL] = pq.config.MsgTtl
	res[PQ_STATUS_DELIVERY_DELAY] = pq.config.DeliveryDelay
	res[PQ_STATUS_POP_LOCK_TIMEOUT] = pq.config.PopLockTimeout
	res[PQ_STATUS_POP_COUNT_LIMIT] = pq.config.PopCountLimit
	res[PQ_STATUS_CREATE_TS] = pq.desc.CreateTs
	res[PQ_STATUS_LAST_PUSH_TS] = pq.config.LastPushTs
	res[PQ_STATUS_LAST_POP_TS] = pq.config.LastPopTs
	res[PQ_STATUS_TOTAL_MSGS] = pq.GetSize()
	res[PQ_STATUS_IN_FLIGHT_MSG] = pq.lockedMsgCnt
	res[PQ_STATUS_AVAILABLE_MSGS] = pq.GetSize() - pq.lockedMsgCnt
	res[PQ_STATUS_FAIL_QUEUE] = pq.config.PopLimitQueueName
	return res
}

func (pq *PQueue) SetParams(msgTtl, maxSize, deliveryDelay, popLimit, lockTimeout int64, failQueue string) IResponse {
	if failQueue != "" {
		if fq := pq.getFailQueue(failQueue); fq == nil {
			return InvalidRequest("PQueue doesn't exist: " + failQueue)
		}
	}
	pq.lock.Lock()
	pq.config.MsgTtl = msgTtl
	pq.config.MaxSize = maxSize
	pq.config.DeliveryDelay = deliveryDelay
	pq.config.PopCountLimit = popLimit
	pq.config.PopLockTimeout = lockTimeout
	if failQueue != "" {
		pq.config.PopLimitQueueName = failQueue
	}
	pq.lock.Unlock()
	SaveServiceConfig(pq.GetServiceId(), pq.config)
	return OK_RESPONSE
}

func (pq *PQueue) GetCurrentStatus() IResponse {
	return NewDictResponse("+STATUS", pq.GetStatus())
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
		for _, v := range pq.id2sn {
			snList = append(snList, v)
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
	pq.closed.SetTrue()
	// This should break a goroutine loop.
	select {
	case pq.popLimitMoveChan <- nil:
	default:
	}
}

func (pq *PQueue) IsClosed() bool {
	return pq.closed.IsTrue()
}

func (pq *PQueue) TimeoutItems(cutOffTs int64) IResponse {
	var total int64
	pq.lock.Lock()

	for value := pq.checkTimeouts(cutOffTs); value > 0; value = pq.checkTimeouts(cutOffTs) {
		total += value
	}

	pq.lock.Unlock()

	return NewIntResponse(total)
}

func (pq *PQueue) ReleaseInFlight(cutOffTs int64) IResponse {
	var total int64
	pq.lock.Lock()

	for value := pq.checkTimeouts(cutOffTs); value > 0; value = pq.checkTimeouts(cutOffTs) {
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
	pq.lock.Lock()
	sn, ok := pq.id2sn[msgId]
	if !ok {
		pq.lock.Unlock()
		return ERR_MSG_NOT_FOUND
	}
	msg := pq.trackHeap.GetMsg(sn)
	data := map[string]interface{}{
		MSG_INFO_ID:        msgId,
		MSG_INFO_LOCKED:    msg.UnlockTs > 0,
		MSG_INFO_UNLOCK_TS: msg.UnlockTs,
		MSG_INFO_POP_COUNT: msg.PopCount,
		MSG_INFO_PRIORITY:  msg.Priority,
		MSG_INFO_EXPIRE_TS: msg.ExpireTs,
	}
	pq.lock.Unlock()
	return NewDictResponse("+MSGINFO", data)
}

func (pq *PQueue) DeleteLockedById(msgId string) IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	sn := pq.id2sn[msgId]

	if sn == 0 {
		return ERR_MSG_NOT_FOUND
	}

	if pq.trackHeap.GetMsg(sn).UnlockTs == 0 {
		return ERR_MSG_NOT_LOCKED
	}

	pq.deleteMessage(sn)
	pq.lockedMsgCnt--

	return OK_RESPONSE
}

func (pq *PQueue) DeleteById(msgId string) IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	sn := pq.id2sn[msgId]
	if sn == 0 {
		return ERR_MSG_NOT_FOUND
	}
	if pq.trackHeap.GetMsg(sn).UnlockTs > 0 {
		return ERR_MSG_IS_LOCKED
	}
	pq.deleteMessage(sn)
	return OK_RESPONSE
}

func (pq *PQueue) Push(msgId, payload string, msgTtl, delay, priority int64) IResponse {

	if pq.config.MaxSize > 0 && int64(len(pq.id2sn)) >= pq.config.MaxSize {
		return ERR_SIZE_EXCEEDED
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
	pq.id2sn[msgId] = sn

	if delay == 0 {
		pq.availMsgs.Push(msg)
	} else {
		msg.UnlockTs = nowTs + delay
		pq.lockedMsgCnt++
	}
	pq.trackHeap.Push(msg)
	// Payload is a race conditional case, since it is not always flushed on disk and may or may not exist in memory.
	pq.payloadLock.Lock()
	pq.StoreFullItemInDB(Sn2Bin(sn), msg.StringMarshal(), payload)
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

		msg := pq.availMsgs.Pop()
		snDb := msg.Sn2Bin()

		if lock {
			pq.lockedMsgCnt++
			msg.UnlockTs = nowTs + lockTimeout
			msg.PopCount += 1
			// Changing priority to -1 guarantees that message will stay at the top of the queue.
			msg.Priority = -1
			pq.trackHeap.Push(msg)
			pq.StoreItemBodyInDB(snDb, msg.StringMarshal())
		} else {
			delete(pq.id2sn, msg.StrId)
		}

		pq.payloadLock.Lock()
		pq.lock.Unlock()
		payload := pq.GetPayloadFromDB(snDb)
		msgs = append(msgs, NewMsgResponseItem(msg, payload))

		if !lock {
			pq.DeleteFullItemFromDB(snDb)
		}

		pq.payloadLock.Unlock()
	}
	return msgs
}

// UpdateLockById sets a user defined message lock timeout.
// It works only for locked messages.
func (pq *PQueue) UpdateLockById(msgId string, lockTimeout int64) IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	sn := pq.id2sn[msgId]
	if sn > 0 {
		msg := pq.trackHeap.GetMsg(sn)
		if msg.UnlockTs > 0 {
			msg.UnlockTs = Uts() + lockTimeout
			pq.trackHeap.Push(msg)
			pq.StoreItemBodyInDB(msg.Sn2Bin(), msg.StringMarshal())
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
	msg := pq.trackHeap.GetMsg(sn)
	if msg.UnlockTs == 0 {
		return ERR_MSG_NOT_LOCKED
	}
	// Message exists, push it into the front of the queue.
	pq.returnToFront(msg)
	return OK_RESPONSE
}

// WARNING: this function acquires lock! It automatically releases lock if message is not found.
func (pq *PQueue) acquireLockAndGetReceiptMessage(rcpt string) (*PQMsgMetaData, *ErrorResponse) {
	parts := strings.SplitN(rcpt, "-", 2)

	if len(parts) != 2 {
		return nil, ERR_INVALID_RECEIPT
	}
	sn, err := Parse36BaseUIntValue(parts[0])
	if err != nil {
		return nil, ERR_INVALID_RECEIPT
	}

	popCount, err := Parse36BaseIntValue(parts[1])
	if err != nil {
		return nil, ERR_INVALID_RECEIPT
	}

	// To improve performance the lock is acquired here. The caller must unlock it.
	pq.lock.Lock()
	msg := pq.trackHeap.GetMsg(sn)
	if msg != nil && msg.UnlockTs > 0 && msg.PopCount == popCount {
		return msg, nil
	}
	pq.lock.Unlock()
	return nil, ERR_RECEIPT_EXPIRED
}

// UpdateLockByRcpt sets a user defined message lock timeout tp the message that matches receipt.
func (pq *PQueue) UpdateLockByRcpt(rcpt string, lockTimeout int64) IResponse {
	// This call may acquire lock.
	msg, err := pq.acquireLockAndGetReceiptMessage(rcpt)
	if err != nil {
		return err
	}
	msg.UnlockTs = Uts() + lockTimeout
	pq.trackHeap.Push(msg)
	pq.StoreItemBodyInDB(msg.Sn2Bin(), msg.StringMarshal())
	pq.lock.Unlock()
	return OK_RESPONSE
}

func (pq *PQueue) DeleteByReceipt(rcpt string) IResponse {
	// This call may acquire lock.
	msg, err := pq.acquireLockAndGetReceiptMessage(rcpt)
	if err != nil {
		return err
	}
	pq.deleteMessage(msg.SerialNumber)
	pq.lock.Unlock()
	return OK_RESPONSE
}

func (pq *PQueue) UnlockByReceipt(rcpt string) IResponse {
	// This call may acquire lock.
	msg, err := pq.acquireLockAndGetReceiptMessage(rcpt)
	if err != nil {
		return err
	}
	pq.returnToFront(msg)
	pq.lock.Unlock()
	return OK_RESPONSE
}

func (pq *PQueue) deleteMessage(sn uint64) bool {
	if msg := pq.trackHeap.Remove(sn); msg != nil {
		// message that has UnlockTs > 0 must not be in avail msgs queue.
		if msg.UnlockTs == 0 {
			pq.availMsgs.Remove(sn)
		}
		delete(pq.id2sn, msg.StrId)
		pq.payloadLock.Lock()
		pq.DeleteFullItemFromDB(msg.Sn2Bin())
		pq.payloadLock.Unlock()
		return true
	}
	return false
}

func (pq *PQueue) getFailQueue(name string) *PQueue {
	// Get service and make sure it is still available.
	popQ, ok := pq.svcs.GetService(name)
	if !ok {
		log.Debug("No '%s' queue to push messages (exceeded pop limit) from '%s'",
			pq.config.PopLimitQueueName, pq.desc.Name)
		return nil
	}
	// Make sure retrieved service has an appropriate type.
	popLimitPq, ok := popQ.(*PQueue)
	if !ok {
		log.Debug("'%s' has a wrong type to push pop limit exceeded messages from '%s'",
			pq.config.PopLimitQueueName, pq.desc.Name)
		return nil
	}
	return popLimitPq
}

func (pq *PQueue) moveToPopLimitedQueue() {
	log.Debug("%s: Starting pop limit loop", pq.desc.Name)
	var msg *PQMsgMetaData
	for pq.closed.IsFalse() {
		select {
		case msg = <-pq.popLimitMoveChan:
		case <-GetQuitChan():
			break
		}

		// Nil can be received only in case of service is closing.
		if msg == nil {
			return
		}

		popLimitPq := pq.getFailQueue(pq.config.PopLimitQueueName)
		if popLimitPq == nil {
			pq.config.PopLimitQueueName = ""
			SaveServiceConfig(pq.GetServiceId(), pq.config)
			break
		}

		// Make sure service is not closed while we are pushing messages into it.
		popLimitPq.closed.Lock()
		for msg != nil {
			binSn := msg.Sn2Bin()
			popLimitPq.Push(msg.StrId,
				pq.GetPayloadFromDB(binSn),
				popLimitPq.config.MsgTtl,
				popLimitPq.config.DeliveryDelay,
				msg.Priority)
			pq.DeleteFullItemFromDB(binSn)
			select {
			case msg = <-pq.popLimitMoveChan:
				// Same thing. Service is closing.
				if msg == nil {
					return
				}
			default:
				msg = nil
			}
		}
		popLimitPq.closed.Unlock()
	}
	log.Debug("%s: Finishing pop limit loop", pq.desc.Name)
}

// Attempts to return a message into the front of the queue.
// If a number of POP attempts has exceeded, message will be deleted.
func (pq *PQueue) returnToFront(msg *PQMsgMetaData) {
	pq.lockedMsgCnt--
	popLimit := pq.config.PopCountLimit
	if popLimit > 0 && msg.PopCount >= popLimit {
		if pq.config.PopLimitQueueName == "" {
			pq.deleteMessage(msg.SerialNumber)
		} else {
			pq.trackHeap.Remove(msg.SerialNumber)
			delete(pq.id2sn, msg.StrId)
			pq.popLimitMoveChan <- msg
		}
	} else {
		msg.UnlockTs = 0
		pq.availMsgs.Push(msg)
		pq.trackHeap.Push(msg)
		pq.StoreItemBodyInDB(msg.Sn2Bin(), msg.StringMarshal())
	}
}

func (pq *PQueue) CheckTimeouts(ts int64) IResponse {
	return NewIntResponse(pq.checkTimeouts(ts))
}

// Unlocks all items which exceeded their lock time.
func (pq *PQueue) checkTimeouts(ts int64) int64 {
	h := pq.trackHeap
	var cntDel int64 = 0
	var cntRet int64 = 0
	for h.NotEmpty() && cntDel+cntRet < CFG_PQ.TimeoutCheckBatchSize {
		msg := h.MinMsg()
		if msg.UnlockTs > 0 {
			if msg.UnlockTs < ts {
				cntRet++
				pq.returnToFront(msg)
			} else {
				break
			}
		} else if msg.ExpireTs < ts {
			cntDel++
			pq.deleteMessage(msg.SerialNumber)
		} else {
			break
		}
	}
	if cntRet > 0 {
		NewMessageNotify(pq.newMsgNotification)
		log.Debug("%d item(s) moved to the queue.", cntRet)
	}
	if cntDel > 0 {
		log.Debug("%d item(s) removed from the queue.", cntDel)
	}
	return cntDel + cntRet
}

func (pq *PQueue) loadAllMessages() {
	nowTs := Uts()
	log.Debug("Initializing queue: %s", pq.desc.Name)
	msgIter := pq.GetItemIterator()
	delSn := []uint64{}

	for ; msgIter.Valid(); msgIter.Next() {
		sn := DecodeBytesToUnit64(msgIter.GetTrimKey())
		msg := UnmarshalPQMsgMetaData(sn, msgIter.GetValue())
		// Message data has errors.
		if msg == nil {
			continue
		}

		// Store list if message IDs that should be removed.
		if msg.ExpireTs <= nowTs && msg.UnlockTs == 0 {
			delSn = append(delSn, sn)
		} else {
			// Don't count expired message to figure out the serial number.
			// It may happen so that there is a chance to even reset a serial number
			if sn > pq.msgSerialNumber {
				pq.msgSerialNumber = sn
			}
			pq.id2sn[msg.StrId] = sn
			pq.trackHeap.Push(msg)
			if msg.UnlockTs == 0 {
				pq.availMsgs.Push(msg)
			} else {
				pq.lockedMsgCnt++
			}
		}
	}

	msgIter.Close()

	if len(delSn) > 0 {
		log.Debug("Deleting %d expired messages", len(delSn))
		for _, dsn := range delSn {
			pq.DeleteFullItemFromDB(Sn2Bin(dsn))
		}
	}

	log.Debug("Total messages: %d", len(pq.id2sn))
	log.Debug("Locked messages: %d", pq.lockedMsgCnt)
	log.Debug("Available messages: %d", pq.availMsgs.Len())
}

var _ ISvc = &PQueue{}
