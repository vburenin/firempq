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
	"github.com/vburenin/firempq/qconf"
	"github.com/vburenin/firempq/signals"
	"github.com/vburenin/firempq/utils"
	"go.uber.org/zap"
)

type PQueue struct {
	MessagePool
	// A must attribute of each service containing all essential service information generated upon creation.
	desc *qconf.QueueDescription
	// Configuration setting on a queue level.
	config *qconf.QueueConfig

	// Database storage
	db apis.DataStorage

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

	configUpdater func(config *qconf.QueueParams) (*qconf.QueueConfig, error)
}

func NewPQueue(
	db apis.DataStorage,
	desc *qconf.QueueDescription,
	config *qconf.QueueConfig,
	deadMsgChan chan DeadMessage,
	configUpdater func(config *qconf.QueueParams) (*qconf.QueueConfig, error)) *PQueue {

	q := &PQueue{
		desc:               desc,
		config:             config,
		db:                 db,
		id2msg:             make(map[string]*pmsg.MsgMeta),
		availMsgs:          NewMsgQueue(),
		timeoutHeap:        NewTimeoutHeap(),
		newMsgNotification: make(chan struct{}),
		deadMsgChan:        deadMsgChan,
		configUpdater:      configUpdater,
	}
	q.InitPool(1000)
	return q
}

func (pq *PQueue) ConnScope(rw apis.ResponseWriter) *ConnScope {
	return NewConnScope(pq, rw)
}

// StartUpdate runs a loop of periodic data updates.
func (pq *PQueue) Update() int64 {
	cnt := pq.checkTimeouts(utils.Uts())
	return cnt
}

// ServiceConfig returns service config as an empty interface type.
// User service type getter to find out the expected config type.
func (pq *PQueue) Config() *qconf.QueueConfig {
	return pq.config
}

// Description is queue description.
func (pq *PQueue) Description() *qconf.QueueDescription {
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

func (pq *PQueue) UpdateConfig(config *qconf.QueueParams) apis.IResponse {
	if pq.configUpdater == nil {
		return mpqerr.ErrDbProblem
	}
	updatedConf, err := pq.configUpdater(config)
	if err != nil {
		return mpqerr.ErrDbProblem
	}
	pq.lock.Lock()
	pq.config = updatedConf
	pq.lock.Unlock()
	return resp.OK
}

func (pq *PQueue) GetCurrentStatus() apis.IResponse {
	return resp.NewDictResponse("+STATUS", pq.GetStatus())
}

// Clear drops all locked and unlocked messages in the queue.
func (pq *PQueue) Clear() {
	total := 0
	pq.lock.Lock()
	pq.WriteWipeAll()
	pq.id2msg = make(map[string]*pmsg.MsgMeta, 4096)
	pq.availMsgs.Clear()
	pq.lock.Unlock()
	log.Debug("messages removed", zap.Int("count", total))
}

func (pq *PQueue) TimeoutItems(cutOffTs int64) apis.IResponse {
	var total int64

	for value := pq.checkTimeouts(cutOffTs); value > 0; value = pq.checkTimeouts(cutOffTs) {
		total += value
	}

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
	msg, ok := pq.id2msg[msgId]

	if !ok {
		pq.lock.Unlock()
		return mpqerr.ErrMsgNotFound
	}

	if msg.UnlockTs == 0 {
		pq.lock.Unlock()
		return mpqerr.ErrMsgNotLocked
	}

	sn := msg.Serial
	pq.lockedMsgCnt--
	pq.timeoutHeap.Remove(msg.Serial)
	delete(pq.id2msg, msg.StrId)
	pq.ReturnMessage(msg)
	pq.lock.Unlock()

	pq.WriteDeleteMetadata(sn)

	return resp.OK
}

func (pq *PQueue) DeleteById(msgId string) apis.IResponse {
	pq.lock.Lock()

	msg, ok := pq.id2msg[msgId]
	if !ok {
		pq.lock.Unlock()
		return mpqerr.ErrMsgNotFound
	}
	if msg.UnlockTs > 0 {
		pq.lock.Unlock()
		return mpqerr.ErrMsgLocked
	}

	sn := msg.Serial
	pq.lockedMsgCnt--
	pq.timeoutHeap.Remove(msg.Serial)
	delete(pq.id2msg, msg.StrId)
	pq.ReturnMessage(msg)
	pq.lock.Unlock()

	pq.WriteDeleteMetadata(sn)

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
	pq.WriteAddMetadata(msg)

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

	pq.lock.Lock()
	_, ok := pq.id2msg[msgId]
	if ok {
		pq.lock.Unlock()
		return mpqerr.ErrMsgAlreadyExists
	}

	// pre-creating message before grabbing a lock
	pq.msgSerialNumber++

	msg := pq.AllocateNewMessage()
	msg.StrId = msgId
	msg.ExpireTs = nowTs + msgTtl
	msg.Serial = pq.msgSerialNumber

	if delay > 0 {
		msg.UnlockTs = nowTs + delay
	}

	pq.id2msg[msgId] = msg
	pq.lock.Unlock()

	payloadID, payloadOffset, err := pq.db.AddPayload(enc.UnsafeStringToBytes(payload))

	pq.lock.Lock()
	if err != nil {
		delete(pq.id2msg, msgId)
		pq.lock.Unlock()
		return mpqerr.ErrDbProblem
	}

	msg.PayloadFileId = payloadID
	msg.PayloadOffset = payloadOffset
	pq.lock.Unlock()

	pq.WriteAddMetadata(msg)

	pq.lock.Lock()
	if delay == 0 {
		pq.availMsgs.Add(msg)
	}

	pq.timeoutHeap.Push(msg)
	pq.lock.Unlock()

	signals.NewMessageNotify(pq.newMsgNotification)

	return resp.NewMsgResponse(msgId)
}

func (pq *PQueue) SyncWait() {
	pq.db.SyncWait()
}

func (pq *PQueue) popMessages(lockTimeout int64, limit int64, lock bool) []*pmsg.FullMessage {
	nowTs := utils.Uts()
	msgs := make([]*pmsg.FullMessage, 0, 10)

	atomic.StoreInt64(&pq.config.LastPopTs, nowTs)
	for int64(len(msgs)) < limit {
		pq.lock.Lock()
		msg := pq.availMsgs.Pop()
		if msg == nil {
			pq.lock.Unlock()
			return msgs
		}

		pq.lock.Unlock()
		// TODO(vburenin): Log ignored error
		payload, _ := pq.db.RetrievePayload(msg.PayloadFileId, msg.PayloadOffset)
		pq.lock.Lock()

		if lock {
			pq.lockedMsgCnt++
			msg.UnlockTs = nowTs + lockTimeout
			msg.PopCount++
			pq.timeoutHeap.Push(msg)
			msgs = append(msgs, pmsg.NewFullMessage(msg, payload))
			pq.lock.Unlock()

			pq.WriteUpdateMetadata(msg)
		} else {
			sn := msg.Serial
			delete(pq.id2msg, msg.StrId)
			pq.timeoutHeap.Remove(msg.Serial)
			msgs = append(msgs, pmsg.NewFullMessage(msg, payload))
			pq.ReturnMessage(msg)
			pq.lock.Unlock()

			pq.WriteDeleteMetadata(sn)
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
		pq.WriteUpdateMetadata(msg)
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
		pq.WriteUpdateMetadata(msg)
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

	sn := msg.Serial
	pq.lockedMsgCnt--
	pq.timeoutHeap.Remove(msg.Serial)
	delete(pq.id2msg, msg.StrId)
	pq.ReturnMessage(msg)
	pq.lock.Unlock()

	pq.WriteDeleteMetadata(sn)
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
		pq.WriteDeleteMetadata(msg.Serial)

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
		pq.WriteUpdateMetadata(msg)
	}
}

func (pq *PQueue) CheckTimeouts(ts int64) apis.IResponse {
	r := resp.NewIntResponse(pq.checkTimeouts(ts))
	return r
}

// Unlocks all items which exceeded their lock time.
func (pq *PQueue) checkTimeouts(ts int64) int64 {
	h := pq.timeoutHeap
	var cntDel int64 = 0
	var cntRet int64 = 0

	for {
		pq.lock.Lock()
		if h.Empty() {
			pq.lock.Unlock()
			break
		}

		msg := h.MinMsg()
		if msg.ExpireTs < ts {
			cntDel++
			sn := msg.Serial
			pq.lockedMsgCnt--
			pq.timeoutHeap.Remove(msg.Serial)
			delete(pq.id2msg, msg.StrId)
			pq.ReturnMessage(msg)
			pq.lock.Unlock()

			pq.WriteDeleteMetadata(sn)
		} else if msg.UnlockTs > 0 && msg.UnlockTs < ts {
			cntRet++
			pq.returnToFront(msg)
			pq.lock.Unlock()
		} else {
			pq.lock.Unlock()
			break
		}

		if cntDel+cntRet > conf.CFG_PQ.TimeoutCheckBatchSize {
			pq.lock.Unlock()
			break
		}
	}

	if cntRet > 0 {
		signals.NewMessageNotify(pq.newMsgNotification)
	}

	return cntDel + cntRet
}

func (pq *PQueue) LoadMessages(ctx *fctx.Context, msgs []*pmsg.MsgMeta) {
	ts := utils.Uts()
	ctx.Debug("initializing queue with messages", zap.Int("count", len(msgs)))
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
	ctx.Debug("messages loaded",
		zap.Int("total", len(pq.id2msg)),
		zap.Uint64("locked", pq.lockedMsgCnt),
		zap.Uint64("available", pq.availMsgs.Size()))
}

func (pq *PQueue) Close() error {
	return pq.db.Close()
}

func (pq *PQueue) WriteAddMetadata(m *pmsg.MsgMeta) {
	data := make([]byte, m.Size()+1)
	data[0] = DBActionAddMetadata
	n, _ := m.MarshalTo(data[1:])
	final := data[:n+1]
	pq.db.AddMetadata(final)
}

func (pq *PQueue) WriteUpdateMetadata(m *pmsg.MsgMeta) {
	data := make([]byte, m.Size()+1)
	data[0] = DBActionUpdateMetadata
	n, _ := m.MarshalTo(data[1:])
	pq.db.AddMetadata(data[:n+1])
}

func (pq *PQueue) WriteDeleteMetadata(sn uint64) {
	data := make([]byte, 1+8)
	data[0] = DBActionDeleteMetadata
	enc.Uint64ToBin(sn, data[1:])
	pq.db.AddMetadata(data)
}

func (pq *PQueue) WriteWipeAll() {
	pq.db.AddMetadata([]byte{DBActionWipeAll})
}
