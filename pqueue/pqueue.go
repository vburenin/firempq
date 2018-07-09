package pqueue

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/mpqproto"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/pmsg"
	"github.com/vburenin/firempq/queue_info"
	"github.com/vburenin/firempq/signals"
	"github.com/vburenin/firempq/utils"
)

type PQueue struct {
	// A must attribute of each service containing all essential service information generated upon creation.
	desc *queue_info.ServiceDescription
	// Configuration setting on a queue level.
	config *conf.PQConfig

	// Database storage
	db apis.DataStorage
	// A wrapper on top of common database operations.
	metadb *MetaActionDB

	// For messages jumping around all their jumps should be protected.
	lock sync.Mutex

	// Currently available messages to be popped.
	availMsgs *MessageQueue

	// A heap of messages sorted by their expiration or unlock timestamp.
	timeoutHeap *TimeoutHeap

	// map of messages accessible by their string id. one of the uses is for deduplication purposes.
	id2msg map[string]*pmsg.MsgMeta

	// This queue will be used to push messages exceeded pop limit attempts. All errors are ignored.
	deadMsgChan chan DeadMessage

	// Channel that used to notify about new messages added to the queue.
	newMsgNotification chan struct{}

	// Serial number counter use to assign one for each new message.
	msgSerialNumber uint64

	// Number of locked messages
	lockedMsgCnt uint64
}

func NewPQueue(
	db apis.DataStorage,
	desc *queue_info.ServiceDescription,
	deadMsgChan chan DeadMessage,
	config *conf.PQConfig) *PQueue {

	return &PQueue{
		desc:               desc,
		config:             config,
		db:                 db,
		metadb:             NewDBFunctor(desc.ExportId, db),
		id2msg:             make(map[string]*pmsg.MsgMeta),
		availMsgs:          NewMsgQueue(),
		timeoutHeap:        NewTimeoutHeap(),
		newMsgNotification: make(chan struct{}),
		deadMsgChan:        deadMsgChan,
	}
}

func (pq *PQueue) ConnScope(rw apis.ResponseWriter) *ConnScope {
	return NewConnScope(pq, rw)
}

// StartUpdate runs a loop of periodic data updates.
func (pq *PQueue) Update() int64 {
	pq.lock.Lock()
	cnt := pq.checkTimeouts(utils.Uts())
	pq.lock.Unlock()
	return cnt
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
	res[StatusQueueMaxSize] = pq.config.MaxMsgsInQueue
	res[StatusQueuePopWaitTimeout] = pq.config.PopWaitTimeout
	res[StatusQueueMsgTTL] = pq.config.MsgTtl
	res[StatusQueueMaxMsgSize] = pq.config.MaxMsgSize
	res[StatusQueueDeliveryDelay] = pq.config.DeliveryDelay
	res[StatusQueuePopLockTimeout] = pq.config.PopLockTimeout
	res[StatusQueuePopCountLimit] = pq.config.PopCountLimit
	res[StatusQueueCreateTs] = pq.desc.CreateTs
	res[StatusQueueLastPushTs] = atomic.LoadInt64(&pq.config.LastPushTs)
	res[StatusQueueLastPopTs] = atomic.LoadInt64(&pq.config.LastPopTs)
	res[StatusQueueTotalMsgs] = totalMsg
	res[StatusQueueInFlightMsgs] = lockedCount
	res[StatusQueueAvailableMsgs] = totalMsg - lockedCount
	res[StatusQueueDeadMsgQueue] = pq.config.PopLimitQueueName
	return res
}

type QueueParams struct {
	MsgTTL         *int64
	MaxMsgSize     *int64
	MaxMsgsInQueue *int64
	DeliveryDelay  *int64
	PopCountLimit  *int64
	PopLockTimeout *int64
	PopWaitTimeout *int64
	FailQueue      *string
}

func (pq *PQueue) SetParams(params *QueueParams) apis.IResponse {
	pq.lock.Lock()

	if params.FailQueue != nil {
		pq.config.PopLimitQueueName = *params.FailQueue
	}

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

func (pq *PQueue) TimeoutItems(cutOffTs int64) apis.IResponse {
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
	MsgInfoID       = "Id"
	MsgInfoLocked   = "Locked"
	MsgInfoUnlockTs = "UnlockTs"
	MsgInfoPopCount = "PopCount"
	MsgInfoExpireTs = "ExpireTs"
)

func (pq *PQueue) GetMessageInfo(msgId string) apis.IResponse {
	pq.lock.Lock()
	msg, ok := pq.id2msg[msgId]
	if !ok {
		pq.lock.Unlock()
		return mpqerr.ErrMsgNotFound
	}
	data := map[string]interface{}{
		MsgInfoID:       msgId,
		MsgInfoLocked:   msg.UnlockTs > 0,
		MsgInfoUnlockTs: msg.UnlockTs,
		MsgInfoPopCount: msg.PopCount,
		MsgInfoExpireTs: msg.ExpireTs,
	}
	pq.lock.Unlock()
	return resp.NewDictResponse("+MSGINFO", data)
}

func (pq *PQueue) DeleteLockedById(msgId string) apis.IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	msg, ok := pq.id2msg[msgId]

	if !ok {
		return mpqerr.ErrMsgNotFound
	}

	if msg.UnlockTs == 0 {
		return mpqerr.ErrMsgNotLocked
	}

	pq.deleteMessage(msg)
	pq.lockedMsgCnt--

	return resp.OK
}

func (pq *PQueue) DeleteById(msgId string) apis.IResponse {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	msg, ok := pq.id2msg[msgId]
	if !ok {
		return mpqerr.ErrMsgNotFound
	}
	if msg.UnlockTs > 0 {
		return mpqerr.ErrMsgLocked
	}
	pq.deleteMessage(msg)
	return resp.OK
}

func (pq *PQueue) AddExistingMessage(msg *pmsg.MsgMeta) error {
	if pq.config.MaxMsgsInQueue > 0 && int64(len(pq.id2msg)) >= pq.config.MaxMsgsInQueue {
		return mpqerr.ErrSizeExceeded
	}
	nowTs := utils.Uts()
	atomic.StoreInt64(&pq.config.LastPushTs, nowTs)

	msg = &pmsg.MsgMeta{
		StrId:         msg.StrId,
		PayloadOffset: msg.PayloadOffset,
		PayloadFileId: msg.PayloadFileId,

		ExpireTs: nowTs + pq.config.MsgTtl,
		UnlockTs: 0,
	}

	pq.lock.Lock()
	pq.msgSerialNumber++
	msg.Serial = pq.msgSerialNumber
	pq.id2msg[msg.StrId] = msg
	pq.lock.Unlock()
	pq.metadb.AddMetadata(msg)

	pq.lock.Lock()
	pq.timeoutHeap.Push(msg)
	pq.availMsgs.Add(msg)
	pq.lock.Unlock()
	signals.NewMessageNotify(pq.newMsgNotification)

	return nil
}

func (pq *PQueue) Push(msgId, payload string, msgTtl, delay int64) apis.IResponse {
	var err error
	if pq.config.MaxMsgsInQueue > 0 && int64(len(pq.id2msg)) >= pq.config.MaxMsgsInQueue {
		return mpqerr.ErrSizeExceeded
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
		return mpqerr.ErrMsgAlreadyExists
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
		return mpqerr.ErrDbProblem
	}

	if delay == 0 {
		pq.availMsgs.Add(msg)
	}
	pq.metadb.AddMetadata(msg)
	pq.timeoutHeap.Push(msg)
	pq.lock.Unlock()

	signals.NewMessageNotify(pq.newMsgNotification)

	return resp.NewMsgResponse(msgId)
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
		r = mpqerr.ErrMsgNotFound
	} else if msg.UnlockTs == 0 {
		r = mpqerr.ErrMsgNotLocked
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
		return mpqerr.ErrMsgNotFound
	}
	if msg.UnlockTs == 0 {
		return mpqerr.ErrMsgNotLocked
	}
	// Message exists, push it into the front of the queue.
	pq.returnToFront(msg)
	return resp.OK
}

// WARNING: this function acquires lock! It automatically releases lock if message is not found.
func (pq *PQueue) getMessageByRcpt(rcpt string) (*pmsg.MsgMeta, *mpqerr.ErrorResponse) {
	parts := strings.SplitN(rcpt, "-", 2)

	if len(parts) != 2 {
		return nil, mpqerr.ErrInvalidRcpt
	}
	sn, err := mpqproto.Parse36BaseUIntValue(parts[0])
	if err != nil {
		return nil, mpqerr.ErrInvalidRcpt
	}

	popCount, err := mpqproto.Parse36BaseIntValue(parts[1])
	if err != nil {
		return nil, mpqerr.ErrInvalidRcpt
	}

	// To improve performance the lock is acquired here. The caller must unlock it.
	pq.lock.Lock()
	msg := pq.timeoutHeap.GetMsg(sn)

	if msg != nil && msg.PopCount == popCount {
		return msg, nil
	}

	pq.lock.Unlock()
	return nil, mpqerr.ErrExpiredRcpt
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
		return mpqerr.ErrInvalidRcpt
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
	pq.deleteMessage(msg)
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

func (pq *PQueue) deleteMessage(msg *pmsg.MsgMeta) {
	pq.timeoutHeap.Remove(msg.Serial)
	delete(pq.id2msg, msg.StrId)
	pq.metadb.DeleteMetadata(msg.Serial)
	msg.Serial = 0
}

// Attempts to return a message into the front of the queue.
// If a number of POP attempts has exceeded, message will be deleted.
func (pq *PQueue) returnToFront(msg *pmsg.MsgMeta) {
	if msg.PopCount > 0 {
		pq.lockedMsgCnt--
	}
	popLimit := pq.config.PopCountLimit
	if popLimit > 0 && msg.PopCount >= popLimit {
		pq.timeoutHeap.Remove(msg.Serial)
		delete(pq.id2msg, msg.StrId)
		pq.metadb.DeleteMetadata(msg.Serial)

		msg.Serial = 0
		if pq.config.PopLimitQueueName != "" {
			pq.deadMsgChan <- DeadMessage{
				msg:    msg,
				target: pq.config.PopLimitQueueName,
			}
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
	pq.lock.Lock()
	r := resp.NewIntResponse(pq.checkTimeouts(ts))
	pq.lock.Unlock()
	return r
}

// Unlocks all items which exceeded their lock time.
func (pq *PQueue) checkTimeouts(ts int64) int64 {
	h := pq.timeoutHeap
	var cntDel int64 = 0
	var cntRet int64 = 0
	for h.NotEmpty() && cntDel+cntRet < conf.CFG_PQ.TimeoutCheckBatchSize {
		msg := h.MinMsg()
		if msg.ExpireTs < ts {
			cntDel++
			pq.deleteMessage(msg)
		} else if msg.UnlockTs > 0 && msg.UnlockTs < ts {
			cntRet++
			pq.returnToFront(msg)
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

func (pq *PQueue) LoadMessages(ctx *fctx.Context, msgs []*pmsg.MsgMeta) {
	ctx = fctx.WithParent(ctx, pq.desc.Name)
	ts := utils.Uts()
	ctx.Debug("initializing queue...")
	pq.msgSerialNumber = 0
	if len(msgs) > 0 {
		pq.msgSerialNumber = msgs[len(msgs)-1].Serial
	}
	expired := uint64(0)

	preallocateSize := int(float64(len(msgs)) * 1.3)
	pq.id2msg = make(map[string]*pmsg.MsgMeta, preallocateSize)
	pq.timeoutHeap = SizedTimeoutHeap(preallocateSize)

	for _, msg := range msgs {
		if msg.ExpireTs < ts {
			expired++
			continue
		}

		// Message can get unlocked by this time, so check if it unlocked and place it to the right queue.
		if msg.UnlockTs > 0 && msg.UnlockTs <= ts {
			msg.UnlockTs = 0
		}

		pq.id2msg[msg.StrId] = msg
		pq.timeoutHeap.Push(msg)

		if msg.UnlockTs == 0 {
			pq.availMsgs.Add(msg)
		} else if msg.PopCount > 0 {
			pq.lockedMsgCnt++
		}

	}

	ctx.Debugf("total messages: %d", len(pq.id2msg))
	ctx.Debugf("locked messages: %d", pq.lockedMsgCnt)
	ctx.Debugf("available messages: %d", pq.availMsgs.Size())
}
