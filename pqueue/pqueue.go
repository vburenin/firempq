package pqueue

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/mpqproto"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/pmsg"
	"github.com/vburenin/firempq/queue_info"
	"github.com/vburenin/firempq/signals"
	"github.com/vburenin/firempq/utils"
	"github.com/vburenin/nsync"
)

type PQueue struct {
	// A wrapper on top of common database operations.
	db     apis.DataStorage
	metadb *MetaActionDB
	// Payload access should be protected separately.
	payloadLock sync.Mutex
	// For messages jumping around all their jumps should be protected.
	lock sync.Mutex
	// Currently available messages to be popped.
	availMsgs *MessageQueue
	// All messages with the ticking counters except those which are inFlight.
	timeoutHeap *TimeoutHeap
	id2msg      map[string]*pmsg.MsgMeta
	// All locked messages
	sn2locked map[uint64]*pmsg.MsgMeta
	// Set as True if the service is closed.
	closed nsync.SyncFlag
	// Instance of the database.
	config *conf.PQConfig

	queueGetter func(queueName string) *PQueue

	// This queue will be used to push messages exceeded pop limit attempts. All errors are ignored.
	popLimitMoveChan chan *pmsg.MsgMeta

	// A must attribute of each service containing all essential service information generated upon creation.
	desc *queue_info.ServiceDescription
	// Shorter version of service name to identify this service.
	newMsgNotification chan struct{}

	// Serial number assigned to new messages.
	msgSerialNumber uint64

	// Number of message which are locked
	lockedMsgCnt uint64
}

func NewPQueue(
	queueGetter func(queueName string) *PQueue,
	db apis.DataStorage,
	desc *queue_info.ServiceDescription,
	config *conf.PQConfig) *PQueue {

	return &PQueue{
		desc:               desc,
		config:             config,
		queueGetter:        queueGetter,
		id2msg:             make(map[string]*pmsg.MsgMeta),
		availMsgs:          NewMsgQueue(),
		timeoutHeap:        NewTimeoutHeap(),
		newMsgNotification: make(chan struct{}),
		msgSerialNumber:    0,
		lockedMsgCnt:       0,
		popLimitMoveChan:   make(chan *pmsg.MsgMeta, 16384),
		db:                 db,
		metadb:             NewDBFunctor(desc.ExportId, db),
	}
}

func (pq *PQueue) ConnScope(rw apis.ResponseWriter) *ConnScope {
	return NewConnScope(pq, rw)
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
			if pq.closed.IsUnset() {
				pq.lock.Lock()
				cnt = pq.checkTimeouts(utils.Uts())
				pq.lock.Unlock()
			} else {
				pq.closed.Unlock()
				break
			}
			pq.closed.Unlock()
			if cnt >= conf.CFG_PQ.TimeoutCheckBatchSize {
				time.Sleep(time.Millisecond)
			} else {
				time.Sleep(time.Duration(conf.CFG.UpdateInterval) * time.Millisecond)
			}
		}
	}()
}

// ServiceConfig returns service config as an empty interface type.
// User service type getter to find out the expected config type.
func (pq *PQueue) Config() *conf.PQConfig {
	return pq.config
}

// Description is queue description.
func (pq *PQueue) Description() *queue_info.ServiceDescription {
	return pq.desc
}

// LockedCount is the number of messages which are locked at the moment.
func (pq *PQueue) LockedCount() uint64 {
	return atomic.LoadUint64(&pq.lockedMsgCnt)
}

// DelayedCount is the number of messages which are delayed for delivery.
func (pq *PQueue) DelayedCount() uint64 {
	pq.lock.Lock()
	delayed := uint64(len(pq.id2msg)) - pq.availMsgs.Size() - pq.lockedMsgCnt
	pq.lock.Unlock()
	return delayed
}

// TotalMessages returns a number of all messages currently in the queue.
func (pq *PQueue) TotalMessages() uint64 {
	pq.lock.Lock()
	r := uint64(len(pq.id2msg))
	pq.lock.Unlock()
	return r
}

// AvailableMessages returns number of available messages.
func (pq *PQueue) AvailableMessages() uint64 {
	pq.lock.Lock()
	r := pq.availMsgs.Size()
	pq.lock.Unlock()
	return r
}

// GetStatus returns detailed queue status information.
func (pq *PQueue) GetStatus() map[string]interface{} {
	totalMsg := pq.TotalMessages()
	lockedCount := pq.LockedCount()
	res := make(map[string]interface{})
	res[PQ_STATUS_MAX_QUEUE_SIZE] = pq.config.MaxMsgsInQueue
	res[PQ_STATUS_POP_WAIT_TIMEOUT] = pq.config.PopWaitTimeout
	res[PQ_STATUS_MSG_TTL] = pq.config.MsgTtl
	res[PQ_STATUS_MAX_MSG_SIZE] = pq.config.MaxMsgSize
	res[PQ_STATUS_DELIVERY_DELAY] = pq.config.DeliveryDelay
	res[PQ_STATUS_POP_LOCK_TIMEOUT] = pq.config.PopLockTimeout
	res[PQ_STATUS_POP_COUNT_LIMIT] = pq.config.PopCountLimit
	res[PQ_STATUS_CREATE_TS] = pq.desc.CreateTs
	res[PQ_STATUS_LAST_PUSH_TS] = atomic.LoadInt64(&pq.config.LastPushTs)
	res[PQ_STATUS_LAST_POP_TS] = atomic.LoadInt64(&pq.config.LastPopTs)
	res[PQ_STATUS_TOTAL_MSGS] = totalMsg
	res[PQ_STATUS_IN_FLIGHT_MSG] = lockedCount
	res[PQ_STATUS_AVAILABLE_MSGS] = totalMsg - lockedCount
	res[PQ_STATUS_FAIL_QUEUE] = pq.config.PopLimitQueueName
	return res
}

type PQueueParams struct {
	MsgTTL         *int64
	MaxMsgSize     *int64
	MaxMsgsInQueue *int64
	DeliveryDelay  *int64
	PopCountLimit  *int64
	PopLockTimeout *int64
	PopWaitTimeout *int64
	FailQueue      string
}

func (pq *PQueue) SetParams(params *PQueueParams) apis.IResponse {

	if params.FailQueue != "" {
		if fq := pq.queueGetter(params.FailQueue); fq == nil {
			return mpqerr.InvalidRequest("PQueue doesn't exist: " + params.FailQueue)
		}
		pq.config.PopLimitQueueName = params.FailQueue
	}

	pq.lock.Lock()
	pq.config.LastUpdateTs = utils.Uts()

	if params.MsgTTL != nil {
		pq.config.MsgTtl = *params.MsgTTL
	}
	if params.MaxMsgSize != nil {
		pq.config.MaxMsgSize = *params.MaxMsgSize
	}
	if params.MaxMsgsInQueue != nil {
		pq.config.MaxMsgsInQueue = *params.MaxMsgsInQueue
	}
	if params.DeliveryDelay != nil {
		pq.config.DeliveryDelay = *params.DeliveryDelay
	}
	if params.PopCountLimit != nil {
		pq.config.PopCountLimit = *params.PopCountLimit
	}
	if params.PopLockTimeout != nil {
		pq.config.PopLockTimeout = *params.PopLockTimeout
	}
	if params.PopWaitTimeout != nil {
		pq.config.PopWaitTimeout = *params.PopWaitTimeout
	}
	pq.lock.Unlock()
	queue_info.SaveServiceConfig(pq.desc.ServiceId, pq.config)
	return resp.OK
}

func (pq *PQueue) GetCurrentStatus() apis.IResponse {
	return resp.NewDictResponse("+STATUS", pq.GetStatus())
}

// Clear drops all locked and unlocked messages in the queue.
func (pq *PQueue) Clear() {
	total := 0
	pq.lock.Lock()
	pq.metadb.WipeAll()
	pq.id2msg = make(map[string]*pmsg.MsgMeta, 4096)
	pq.availMsgs.Clear()
	pq.lock.Unlock()
	log.Debug("Removed %d messages.", total)
}

func (pq *PQueue) Close() {
	log.Debug("Closing PQueue service: %s", pq.desc.Name)
	pq.lock.Lock()
	if !pq.IsClosed() {
		pq.closed.Set()
		// This should break a goroutine loop.
		close(pq.popLimitMoveChan)
	}
	pq.lock.Unlock()
}

func (pq *PQueue) IsClosed() bool { return pq.closed.IsSet() }

func (pq *PQueue) TimeoutItems(cutOffTs int64) apis.IResponse {
	var total int64
	pq.lock.Lock()

	for value := pq.checkTimeouts(cutOffTs); value > 0; value = pq.checkTimeouts(cutOffTs) {
		total += value
	}

	pq.lock.Unlock()

	return resp.NewIntResponse(total)
}

func (pq *PQueue) ReleaseInFlight(cutOffTs int64) apis.IResponse {
	var total int64
	pq.lock.Lock()

	for value := pq.checkTimeouts(cutOffTs); value > 0; value = pq.checkTimeouts(cutOffTs) {
		total += value
	}

	pq.lock.Unlock()

	return resp.NewIntResponse(total)
}

// PopWaitItems pops 'limit' messages within 'timeout'(milliseconds) time interval.
func (pq *PQueue) Pop(lockTimeout, popWaitTimeout, limit int64, lock bool) apis.IResponse {
	// Try to pop items first time and return them if number of popped items is greater than 0.
	msgItems := pq.popMessages(lockTimeout, limit, lock)

	if len(msgItems) > 0 || popWaitTimeout == 0 {
		return resp.NewItemsResponse(msgItems)
	}

	endTs := utils.Uts() + popWaitTimeout
	for {
		waitLeft := endTs - utils.Uts()
		select {
		case <-signals.QuitChan:
			return resp.NewItemsResponse(msgItems)
		case <-pq.newMsgNotification:
			msgItems := pq.popMessages(lockTimeout, limit, lock)
			if len(msgItems) > 0 {
				return resp.NewItemsResponse(msgItems)
			}
		case <-time.After(time.Duration(waitLeft) * time.Millisecond):
			return resp.NewItemsResponse(pq.popMessages(lockTimeout, limit, lock))
		}
	}
}

const (
	MSG_INFO_ID        = "Id"
	MSG_INFO_LOCKED    = "Locked"
	MSG_INFO_UNLOCK_TS = "UnlockTs"
	MSG_INFO_POP_COUNT = "PopCount"
	MSG_INFO_EXPIRE_TS = "ExpireTs"
)

func (pq *PQueue) GetMessageInfo(msgId string) apis.IResponse {
	pq.lock.Lock()
	msg, ok := pq.id2msg[msgId]
	if !ok {
		pq.lock.Unlock()
		return mpqerr.ERR_MSG_NOT_FOUND
	}
	data := map[string]interface{}{
		MSG_INFO_ID:        msgId,
		MSG_INFO_LOCKED:    msg.UnlockTs > 0,
		MSG_INFO_UNLOCK_TS: msg.UnlockTs,
		MSG_INFO_POP_COUNT: msg.PopCount,
		MSG_INFO_EXPIRE_TS: msg.ExpireTs,
	}
	pq.lock.Unlock()
	return resp.NewDictResponse("+MSGINFO", data)
}

func (pq *PQueue) DeleteLockedById(msgId string) apis.IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	msg, ok := pq.id2msg[msgId]

	if !ok {
		return mpqerr.ERR_MSG_NOT_FOUND
	}

	if msg.UnlockTs == 0 {
		return mpqerr.ERR_MSG_NOT_LOCKED
	}

	pq.deleteMessage(msg.Serial)
	pq.lockedMsgCnt--

	return resp.OK
}

func (pq *PQueue) DeleteById(msgId string) apis.IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	msg, ok := pq.id2msg[msgId]
	if !ok {
		return mpqerr.ERR_MSG_NOT_FOUND
	}
	if msg.UnlockTs > 0 {
		return mpqerr.ERR_MSG_IS_LOCKED
	}
	pq.deleteMessage(msg.Serial)
	return resp.OK
}

func (pq *PQueue) Push(msgId string, payload string, msgTtl, delay int64) apis.IResponse {
	var err error
	if pq.config.MaxMsgsInQueue > 0 && int64(len(pq.id2msg)) >= pq.config.MaxMsgsInQueue {
		return mpqerr.ERR_SIZE_EXCEEDED
	}

	nowTs := utils.Uts()
	atomic.StoreInt64(&pq.config.LastPushTs, nowTs)

	// pre-creating message before grabbing a lock
	msg := &pmsg.MsgMeta{
		StrId:    msgId,
		ExpireTs: nowTs + msgTtl,
	}

	if delay > 0 {
		msg.UnlockTs = nowTs + delay
	}

	pq.lock.Lock()
	_, ok := pq.id2msg[msgId]
	if ok {
		pq.lock.Unlock()
		return mpqerr.ERR_ITEM_ALREADY_EXISTS
	}

	pq.msgSerialNumber++
	msg.Serial = pq.msgSerialNumber
	pq.id2msg[msgId] = msg
	pq.lock.Unlock()

	msg.PayloadFileId, msg.PayloadOffset, err = pq.db.AddPayload(enc.UnsafeStringToBytes(payload))

	pq.lock.Lock()
	if err != nil {
		delete(pq.id2msg, msgId)
		pq.lock.Unlock()
		return mpqerr.ERR_DB_PROBLEM
	}

	if delay == 0 {
		pq.availMsgs.Add(msg)
	}
	pq.metadb.AddMetadata(msg)
	pq.timeoutHeap.Push(msg)
	pq.lock.Unlock()

	signals.NewMessageNotify(pq.newMsgNotification)

	return resp.OK
}

func (pq *PQueue) popMessages(lockTimeout int64, limit int64, lock bool) []apis.IResponseItem {
	nowTs := utils.Uts()
	var msgs []apis.IResponseItem

	atomic.StoreInt64(&pq.config.LastPopTs, nowTs)

	for int64(len(msgs)) < limit {
		pq.lock.Lock()
		msg := pq.availMsgs.Pop()
		if msg == nil {
			pq.lock.Unlock()
			return msgs
		}
		if lock {
			pq.lockedMsgCnt++
			msg.UnlockTs = nowTs + lockTimeout
			msg.PopCount++
			pq.timeoutHeap.Push(msg)
			pq.lock.Unlock()

			pq.metadb.UpdateMetadata(msg)
		} else {
			delete(pq.id2msg, msg.StrId)
			pq.timeoutHeap.Remove(msg.Serial)
			pq.lock.Unlock()
		}
		// TODO(vburenin): Log ignored error
		payload, _ := pq.db.RetrievePayload(msg.PayloadFileId, msg.PayloadOffset)

		msgs = append(msgs, NewMsgResponseItem(msg, payload))

		if !lock {
			pq.metadb.DeleteMetadata(msg.Serial)
		}
	}
	return msgs
}

// UpdateLockById sets a user defined message lock timeout.
// It works only for locked messages.
func (pq *PQueue) UpdateLockById(msgId string, lockTimeout int64) (r apis.IResponse) {
	pq.lock.Lock()

	msg := pq.id2msg[msgId]
	if msg == nil {
		r = mpqerr.ERR_MSG_NOT_FOUND
	} else if msg.UnlockTs == 0 {
		r = mpqerr.ERR_MSG_NOT_LOCKED
	} else {
		msg.UnlockTs = utils.Uts() + lockTimeout
		pq.timeoutHeap.Push(msg)
		pq.lock.Unlock()
		pq.metadb.UpdateMetadata(msg)
		return resp.OK
	}
	pq.lock.Unlock()
	return r
}

func (pq *PQueue) UnlockMessageById(msgId string) apis.IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	// Make sure message exists.
	msg := pq.id2msg[msgId]
	if msg == nil {
		return mpqerr.ERR_MSG_NOT_FOUND
	}
	if msg.UnlockTs == 0 {
		return mpqerr.ERR_MSG_NOT_LOCKED
	}
	// Message exists, push it into the front of the queue.
	pq.returnToFront(msg)
	return resp.OK
}

// WARNING: this function acquires lock! It automatically releases lock if message is not found.
func (pq *PQueue) getMessageByRcpt(rcpt string) (*pmsg.MsgMeta, *mpqerr.ErrorResponse) {
	parts := strings.SplitN(rcpt, "-", 2)

	if len(parts) != 2 {
		return nil, mpqerr.ERR_INVALID_RECEIPT
	}
	sn, err := mpqproto.Parse36BaseUIntValue(parts[0])
	if err != nil {
		return nil, mpqerr.ERR_INVALID_RECEIPT
	}

	popCount, err := mpqproto.Parse36BaseIntValue(parts[1])
	if err != nil {
		return nil, mpqerr.ERR_INVALID_RECEIPT
	}

	// To improve performance the lock is acquired here. The caller must unlock it.
	pq.lock.Lock()
	msg := pq.timeoutHeap.GetMsg(sn)

	if msg != nil && msg.PopCount == popCount {
		return msg, nil
	}

	pq.lock.Unlock()
	return nil, mpqerr.ERR_RECEIPT_EXPIRED
}

// UpdateLockByRcpt sets a user defined message lock timeout to the message that matches a receipt.
func (pq *PQueue) UpdateLockByRcpt(rcpt string, lockTimeout int64) apis.IResponse {
	// This call may acquire lock.
	msg, err := pq.getMessageByRcpt(rcpt)
	if err != nil {
		return err
	}
	if msg.UnlockTs == 0 {
		if lockTimeout == 0 {
			return resp.OK
		}
		return mpqerr.ERR_INVALID_RECEIPT
	}

	if lockTimeout == 0 {
		msg.UnlockTs = 0
		pq.returnToFront(msg)
	} else {
		msg.UnlockTs = utils.Uts() + lockTimeout
		pq.timeoutHeap.Push(msg)
		pq.metadb.UpdateMetadata(msg)
	}

	pq.lock.Unlock()

	return resp.OK
}

func (pq *PQueue) DeleteByReceipt(rcpt string) apis.IResponse {
	// This call may acquire lock.
	msg, err := pq.getMessageByRcpt(rcpt)
	if err != nil {
		return err
	}
	pq.lockedMsgCnt--
	pq.deleteMessage(msg.Serial)
	pq.lock.Unlock()
	return resp.OK
}

func (pq *PQueue) UnlockByReceipt(rcpt string) apis.IResponse {
	// This call may acquire lock.
	msg, err := pq.getMessageByRcpt(rcpt)
	if err != nil {
		return err
	}
	if msg.UnlockTs > 0 {
		pq.returnToFront(msg)
	}
	pq.lock.Unlock()
	return resp.OK
}

func (pq *PQueue) deleteMessage(sn uint64) bool {
	if msg := pq.timeoutHeap.Remove(sn); msg != nil {
		// Message marked with serial number 0 is considered no longer active.
		msg.Serial = 0
		delete(pq.id2msg, msg.StrId)
		pq.metadb.DeleteMetadata(sn)
		return true
	}
	return false
}

func (pq *PQueue) moveToPopLimitedQueue() {
	log.Debug("%s: Starting pop limit loop", pq.desc.Name)
	defer log.Debug("%s: Finishing pop limit loop", pq.desc.Name)

	var msg *pmsg.MsgMeta
	var ok bool

	for pq.closed.IsUnset() {
		select {
		case msg, ok = <-pq.popLimitMoveChan:
			if !ok {
				return
			}
		case <-signals.QuitChan:
			return
		}

		popLimitPq := pq.queueGetter(pq.config.PopLimitQueueName)
		if popLimitPq == nil {
			pq.config.PopLimitQueueName = ""
			queue_info.SaveServiceConfig(pq.desc.ServiceId, pq.config)
			break
		}

		// Make sure service is not closed while we are pushing messages into it.
		popLimitPq.closed.Lock()
		msgPayload, _ := pq.db.RetrievePayload(msg.PayloadFileId, msg.PayloadOffset)
		popLimitPq.Push(msg.StrId,
			string(msgPayload),
			popLimitPq.config.MsgTtl,
			popLimitPq.config.DeliveryDelay)
		popLimitPq.closed.Unlock()
	}
}

// Attempts to return a message into the front of the queue.
// If a number of POP attempts has exceeded, message will be deleted.
func (pq *PQueue) returnToFront(msg *pmsg.MsgMeta) {
	if msg.PopCount > 0 {
		pq.lockedMsgCnt--
	}
	popLimit := pq.config.PopCountLimit
	if popLimit > 0 && msg.PopCount >= popLimit {
		if pq.config.PopLimitQueueName == "" {
			pq.deleteMessage(msg.Serial)
		} else {
			pq.timeoutHeap.Remove(msg.Serial)
			delete(pq.id2msg, msg.StrId)
			pq.popLimitMoveChan <- msg
		}
	} else {
		msg.UnlockTs = 0
		if msg.PopCount == 0 {
			pq.availMsgs.Add(msg)
		} else {
			pq.availMsgs.Return(msg)
		}
		pq.timeoutHeap.Push(msg)
		pq.metadb.UpdateMetadata(msg)
	}
}

func (pq *PQueue) CheckTimeouts(ts int64) apis.IResponse {
	return resp.NewIntResponse(pq.checkTimeouts(ts))
}

// Unlocks all items which exceeded their lock time.
func (pq *PQueue) checkTimeouts(ts int64) int64 {
	h := pq.timeoutHeap
	var cntDel int64 = 0
	var cntRet int64 = 0
	for h.NotEmpty() && cntDel+cntRet < conf.CFG_PQ.TimeoutCheckBatchSize {
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
			pq.deleteMessage(msg.Serial)
		} else {
			break
		}
	}
	if cntRet > 0 {
		signals.NewMessageNotify(pq.newMsgNotification)
		log.Debug("%d item(s) moved to the queue.", cntRet)
	}
	if cntDel > 0 {
		log.Debug("%d item(s) removed from the queue.", cntDel)
	}
	return cntDel + cntRet
}

type msgArray []*pmsg.MsgMeta

func (m msgArray) Len() int           { return len(m) }
func (m msgArray) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m msgArray) Less(i, j int) bool { return m[i].Serial < m[j].Serial }
