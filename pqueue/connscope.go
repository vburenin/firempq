package pqueue

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/jessevdk/go-flags"
	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/export/proto"
	"github.com/vburenin/firempq/idgen"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/mpqproto"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/qconf"
	"github.com/vburenin/firempq/utils"
)

type ConnScope struct {
	pq             *PQueue
	idGen          *idgen.IdGen
	callsCount     int64
	responseWriter apis.ResponseWriteCloser
	asyncGroup     sync.WaitGroup

	stop int32
}

func NewConnScope(pq *PQueue, r apis.ResponseWriteCloser) *ConnScope {
	return &ConnScope{
		pq:             pq,
		callsCount:     0,
		responseWriter: r,
		idGen:          idgen.NewGen(),
	}
}

func DefaultPQConfig() *qconf.QueueConfig {
	cfg := &conf.Config{}
	flags.ParseArgs(cfg, []string{"firempq"})

	conf.CFG = cfg
	conf.CFG_PQ = &cfg.PQueueConfig

	return &qconf.QueueConfig{
		MaxMsgsInQueue:    conf.CFG_PQ.DefaultMaxQueueSize,
		MsgTtl:            conf.CFG_PQ.DefaultMessageTTL,
		DeliveryDelay:     conf.CFG_PQ.DefaultDeliveryDelay,
		PopLockTimeout:    conf.CFG_PQ.DefaultLockTimeout,
		LastPushTs:        utils.Uts(),
		LastPopTs:         utils.Uts(),
		PopLimitQueueName: "",
		MaxMsgSize:        conf.CFG_PQ.MaxMessageSize,
		PopWaitTimeout:    conf.CFG_PQ.DefaultPopWaitTimeout,
		LastUpdateTs:      utils.Uts(),
	}
}

func ParsePQConfig(params []string) (*qconf.QueueConfig, apis.IResponse) {
	var err *mpqerr.ErrorResponse

	cfg := DefaultPQConfig()
	for len(params) > 0 {
		switch params[0] {
		case proto.CPrmMsgTtl:
			params, cfg.MsgTtl, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxMessageTTL)
		case proto.CPrmMaxMsgSize:
			params, cfg.MaxMsgSize, err = mpqproto.ParseInt64Param(params, 1024, conf.CFG_PQ.MaxMessageSize)
		case proto.CPrmMaxQueueSize:
			params, cfg.MaxMsgsInQueue, err = mpqproto.ParseInt64Param(params, 0, math.MaxInt64)
		case proto.CPrmDeliveryDelay:
			params, cfg.DeliveryDelay, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxDeliveryDelay)
		case proto.CPrmPopLimit:
			params, cfg.PopCountLimit, err = mpqproto.ParseInt64Param(params, 0, math.MaxInt64)
		case proto.CPrmLockTimeout:
			params, cfg.PopLockTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxLockTimeout)
		case proto.CPrmFailQueue:
			params, cfg.PopLimitQueueName, err = mpqproto.ParseItemId(params)
		case proto.CPrmPopWait:
			params, cfg.PopWaitTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxPopWaitTimeout)
		default:
			return nil, mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return nil, err
		}
	}
	return cfg, resp.OK
}

func (cs *ConnScope) Queue() *PQueue {
	return cs.pq
}

// Call dispatches to the command handler to process necessary parameters.
func (cs *ConnScope) Call(cmd string, params []string) apis.IResponse {
	if atomic.LoadInt32(&cs.stop) > 0 {
		return mpqerr.ErrConnClosing
	}
	cs.callsCount += 1
	switch cmd {
	case proto.CmdPopLock:
		return cs.PopLock(params)
	case proto.CmdPop:
		return cs.Pop(params)
	case proto.CmdMsgInfo:
		return cs.GetMessageInfo(params)
	case proto.CmdDeleteByRcpt:
		return cs.DeleteByReceipt(params)
	case proto.CmdUnlockByRcpt:
		return cs.UnlockByReceipt(params)
	case proto.CmdDeleteLockedByID:
		return cs.DeleteLockedById(params)
	case proto.CmdDeleteByID:
		return cs.DeleteById(params)
	case proto.CmdPush:
		return cs.Push(params)
	case proto.CmdPushBatch:
		return cs.PushBatch(params)
	case proto.CmdUpdateLockByID:
		return cs.UpdateLockById(params)
	case proto.CmdUpdateLockByRcpt:
		return cs.UpdateLockByRcpt(params)
	case proto.CmdUnlockByID:
		return cs.UnlockMessageById(params)
	case proto.CmdStats:
		return cs.GetCurrentStatus(params)
	case proto.CmdSetConfig:
		return cs.SetParamValue(params)
	case proto.CmdCheckTimeouts:
		return cs.CheckTimeouts(params)
	case proto.CmdPurge:
		cs.pq.Clear()
		return resp.OK
	}
	return nil
}

// parseMessageIdOnly is looking for message id only.
func parseMessageIdOnly(params []string) (string, *mpqerr.ErrorResponse) {
	if len(params) == 1 {
		if mpqproto.ValidateItemId(params[0]) {
			return params[0], nil
		} else {
			return "", mpqerr.ErrInvalidID
		}
	} else if len(params) == 0 {
		return "", mpqerr.ErrMsgIdNotDefined
	}
	return "", mpqerr.ErrOneIdOnly
}

// parseReceiptOnly is looking for message receipt only.
func parseReceiptOnly(params []string) (string, *mpqerr.ErrorResponse) {
	if len(params) == 1 {
		return params[0], nil
	} else if len(params) > 1 {
		return "", mpqerr.ErrOneRcptOnly
	}
	return "", mpqerr.ErrNoRcpt
}

// PopLock gets message from the queue setting lock timeout.
func (cs *ConnScope) PopLock(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var limit int64 = 1
	var asyncId string

	popWaitTimeout := cs.pq.config.PopWaitTimeout
	lockTimeout := cs.pq.config.PopLockTimeout

	for len(params) > 0 {
		switch params[0] {
		case proto.PrmLockTimeout:
			params, lockTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxLockTimeout)
		case proto.PrmLimit:
			params, limit, err = mpqproto.ParseInt64Param(params, 1, conf.CFG_PQ.MaxPopBatchSize)
		case proto.PrmPopWait:
			params, popWaitTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxPopWaitTimeout)
		case proto.PrmAsync:
			params, asyncId, err = mpqproto.ParseItemId(params)
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	if len(asyncId) > 0 {
		return cs.asyncPop(asyncId, lockTimeout, popWaitTimeout, limit, true)
	} else {
		return cs.pq.Pop(lockTimeout, popWaitTimeout, limit, true)
	}
}

// Pop message from queue completely removing it.
func (cs *ConnScope) Pop(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var limit int64 = 1
	var asyncId string

	popWaitTimeout := cs.pq.config.PopWaitTimeout

	for len(params) > 0 {
		switch params[0] {
		case proto.PrmLimit:
			params, limit, err = mpqproto.ParseInt64Param(params, 1, conf.CFG_PQ.MaxPopBatchSize)
		case proto.PrmPopWait:
			params, popWaitTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxPopWaitTimeout)
		case proto.PrmAsync:
			params, asyncId, err = mpqproto.ParseItemId(params)
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	if len(asyncId) > 0 {
		return cs.asyncPop(asyncId, 0, popWaitTimeout, limit, false)
	} else {
		return cs.pq.Pop(0, popWaitTimeout, limit, false)
	}
}

func (cs *ConnScope) asyncPop(asyncId string, lockTimeout, popWaitTimeout, limit int64, lock bool) apis.IResponse {
	if len(asyncId) != 0 && popWaitTimeout == 0 {
		return resp.NewAsyncResponse(asyncId, mpqerr.ErrAsyncWait)
	}
	go func() {
		cs.asyncGroup.Add(1)
		res := cs.pq.Pop(lockTimeout, popWaitTimeout, limit, lock)
		r := resp.NewAsyncResponse(asyncId, res)
		if err := cs.responseWriter.WriteResponse(r); err != nil {
			log.LogConnError(err)
		}
		cs.asyncGroup.Done()
	}()
	return resp.NewAsyncAccept(asyncId)
}

func (cs *ConnScope) GetMessageInfo(params []string) apis.IResponse {
	msgId, retData := parseMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return cs.pq.GetMessageInfo(msgId)
}

func (cs *ConnScope) DeleteLockedById(params []string) apis.IResponse {
	msgId, retData := parseMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return cs.pq.DeleteLockedById(msgId)
}

func (cs *ConnScope) DeleteById(params []string) apis.IResponse {
	msgId, retData := parseMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return cs.pq.DeleteById(msgId)
}

// DeleteByReceipt deletes locked message using provided receipt.
// This is a preferable method to unlock messages. It helps to avoid
// race condition in case if when message lock has timed out and was
// picked up by other consumer.
func (cs *ConnScope) DeleteByReceipt(params []string) apis.IResponse {
	rcpt, err := parseReceiptOnly(params)
	if err != nil {
		return err
	}
	return cs.pq.DeleteByReceipt(rcpt)
}

// UnlockByReceipt unlocks locked message using provided receipt.
// Unlocking by receipt is making sure message was not relocked
// by something else.
func (cs *ConnScope) UnlockByReceipt(params []string) apis.IResponse {
	rcpt, err := parseReceiptOnly(params)
	if err != nil {
		return err
	}
	return cs.pq.UnlockByReceipt(rcpt)
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (cs *ConnScope) Push(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var msgId string
	var syncWait bool
	var asyncId string
	var payload string

	cfg := cs.pq.config
	delay := cfg.DeliveryDelay
	msgTtl := cfg.MsgTtl

	for len(params) > 0 {
		switch params[0] {
		case proto.PrmID:
			params, msgId, err = mpqproto.ParseUserItemId(params)
		case proto.PrmPayload:
			params, payload, err = mpqproto.ParseStringParam(params, 1, conf.CFG_PQ.MaxMessageSize)
		case proto.PrmDelay:
			params, delay, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxDeliveryDelay)
		case proto.PrmMsgTTL:
			params, msgTtl, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxMessageTTL)
		case proto.PrmSyncWait:
			params = params[1:]
			syncWait = true
		case proto.PrmAsync:
			params, asyncId, err = mpqproto.ParseItemId(params)
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	if len(msgId) == 0 {
		msgId = cs.idGen.RandId()
	}

	if syncWait {
		if len(asyncId) == 0 {
			res := cs.pq.Push(msgId, payload, msgTtl, delay)
			if !res.IsError() {
				cs.pq.SyncWait()
			}
			return res
		} else {
			go func() {
				cs.asyncGroup.Add(1)
				res := cs.pq.Push(msgId, payload, msgTtl, delay)
				if !res.IsError() {
					cs.pq.SyncWait()
				}
				cs.responseWriter.WriteResponse(resp.NewAsyncResponse(asyncId, res))
				cs.asyncGroup.Done()
			}()
			return resp.NewAsyncAccept(asyncId)
		}
	}
	if len(asyncId) > 0 {
		return resp.NewAsyncResponse(asyncId, mpqerr.ErrAsyncPush)
	}
	return cs.pq.Push(msgId, payload, msgTtl, delay)
}

type pushParams struct {
	msgId   string
	payload string
	msgTtl  int64
	delay   int64
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (cs *ConnScope) PushBatch(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var msgId string
	var syncWait bool
	var asyncId string
	var payload string

	var calls [10]pushParams
	var callPos int

	cfg := cs.pq.config
	delay := cfg.DeliveryDelay
	msgTtl := cfg.MsgTtl

	for len(params) > 0 {
		if callPos == len(calls) {
			return mpqerr.InvalidRequest("too many messages in one batch. 10 is the limit.")
		}
		switch params[0] {
		case proto.PrmID:
			params, msgId, err = mpqproto.ParseUserItemId(params)
		case proto.PrmPayload:
			params, payload, err = mpqproto.ParseStringParam(params, 1, conf.CFG_PQ.MaxMessageSize)
		case proto.PrmDelay:
			params, delay, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxDeliveryDelay)
		case proto.PrmMsgTTL:
			params, msgTtl, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxMessageTTL)
		case proto.PrmSyncWait:
			params = params[1:]
			syncWait = true
		case proto.PrmAsync:
			params, asyncId, err = mpqproto.ParseItemId(params)
		case proto.CmdBatchNext:
			params = params[1:]
			if len(msgId) == 0 {
				msgId = cs.idGen.RandId()
			}
			calls[callPos] = pushParams{
				msgId:   msgId,
				payload: payload,
				msgTtl:  msgTtl,
				delay:   delay,
			}
			callPos++
			delay = cfg.DeliveryDelay
			msgTtl = cfg.MsgTtl
			payload = ""
			msgId = ""
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}

	if len(msgId) == 0 {
		msgId = cs.idGen.RandId()
	}

	calls[callPos] = pushParams{
		msgId:   msgId,
		payload: payload,
		msgTtl:  msgTtl,
		delay:   delay,
	}
	callPos++
	responses := make([]apis.IResponse, callPos)

	if len(asyncId) == 0 {
		for i := 0; i < callPos; i++ {
			v := calls[i]
			responses[i] = cs.pq.Push(v.msgId, v.payload, v.msgTtl, v.delay)
		}
		if syncWait {
			cs.pq.SyncWait()
		}
		resp.NewBatchResponse(responses)
	}

	if !syncWait && asyncId != "" {

	}

	/*
			if syncWait {
			if len(asyncId) == 0 {
				res := cs.pq.Push(msgId, Payload, msgTtl, delay)
				if !res.IsError() {
					cs.pq.SyncWait()
				}
				return res
			} else {
				go func() {
					cs.asyncGroup.Add(1)
					res := cs.pq.Push(msgId, Payload, msgTtl, delay)
					if !res.IsError() {
						cs.pq.SyncWait()
					}
					cs.responseWriter.WriteResponse(resp.NewAsyncResponse(asyncId, res))
					cs.asyncGroup.Done()
				}()
				return resp.NewAsyncAccept(asyncId)
			}
		}
	*/
	/*
		if syncWait {
			if len(asyncId) == 0 {
				return res
			} else {
				go func() {
					cs.asyncGroup.Add(1)
					res := cs.pq.Push(msgId, Payload, msgTtl, delay)
					if !res.IsError() {
						// TODO(vburenin): Add flush wait.
						// cs.pq.WaitFlush()
					}
					cs.responseWriter.WriteResponse(resp.NewAsyncResponse(asyncId, res))
					cs.asyncGroup.Done()
				}()
				return resp.NewAsyncAccept(asyncId)
			}
		}*/
	/*if len(asyncId) > 0 {
		return resp.NewAsyncResponse(asyncId, mpqerr.ErrAsyncPush)
	}*/
	return resp.NewBatchResponse(responses)
}

// UpdateLockByRcpt updates message lock according to provided receipt.
func (cs *ConnScope) UpdateLockByRcpt(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var rcpt string
	var lockTimeout int64 = -1

	for len(params) > 0 {
		switch params[0] {
		case proto.PrmReceipt:
			params, rcpt, err = mpqproto.ParseReceiptParam(params)
		case proto.PrmLockTimeout:
			params, lockTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxLockTimeout)
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}

	if len(rcpt) == 0 {
		return mpqerr.ErrNoRcpt
	}

	if lockTimeout < 0 {
		return mpqerr.ErrMsgTimeoutNotDefined
	}
	return cs.pq.UpdateLockByRcpt(rcpt, lockTimeout)
}

// UpdateLockById sets a user defined message lock timeout.
// It works only for locked messages.
func (cs *ConnScope) UpdateLockById(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var msgId string
	var lockTimeout int64 = -1

	for len(params) > 0 {
		switch params[0] {
		case proto.PrmID:
			params, msgId, err = mpqproto.ParseItemId(params)
		case proto.PrmLockTimeout:
			params, lockTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxLockTimeout)
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}

	if len(msgId) == 0 {
		return mpqerr.ErrMsgIdNotDefined
	}

	if lockTimeout < 0 {
		return mpqerr.ErrMsgTimeoutNotDefined
	}
	return cs.pq.UpdateLockById(msgId, lockTimeout)
}

func (cs *ConnScope) UnlockMessageById(params []string) apis.IResponse {
	msgId, retData := parseMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return cs.pq.UnlockMessageById(msgId)
}

func (cs *ConnScope) GetCurrentStatus(params []string) apis.IResponse {
	if len(params) > 0 {
		return mpqerr.ErrCmdNoParamsAllowed
	}
	return cs.pq.GetCurrentStatus()
}

func (cs *ConnScope) CheckTimeouts(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var ts int64 = -1
	for len(params) > 0 {
		switch params[0] {
		case proto.PrmTimeStamp:
			params, ts, err = mpqproto.ParseInt64Param(params, 0, math.MaxInt64)
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	if ts < 0 {
		return mpqerr.ErrNoTsParam
	}
	return cs.pq.CheckTimeouts(ts)
}

func (cs *ConnScope) SetParamValue(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	cfg := cs.pq.config
	msgTtl := cfg.MsgTtl
	maxMsgsInQueue := cfg.MaxMsgsInQueue
	maxMsgSize := cfg.MaxMsgSize
	popLimit := cfg.PopCountLimit
	deliveryDelay := cfg.DeliveryDelay
	lockTimeout := cfg.PopLockTimeout
	failQueue := ""

	if len(params) == 0 {
		return mpqerr.ErrCmdNoParams
	}

	pqParams := &qconf.QueueParams{}

	for len(params) > 0 {
		switch params[0] {
		case proto.CPrmMsgTtl:
			params, msgTtl, err = mpqproto.ParseInt64Param(params, 1, conf.CFG_PQ.MaxMessageTTL)
			pqParams.MsgTTL = &msgTtl
		case proto.CPrmMaxMsgSize:
			params, maxMsgSize, err = mpqproto.ParseInt64Param(params, 1024, conf.CFG_PQ.MaxMessageSize)
			pqParams.MaxMsgSize = &maxMsgSize
		case proto.CPrmMaxQueueSize:
			params, maxMsgsInQueue, err = mpqproto.ParseInt64Param(params, 0, math.MaxInt64)
			pqParams.MaxMsgsInQueue = &maxMsgsInQueue
		case proto.CPrmDeliveryDelay:
			params, deliveryDelay, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxDeliveryDelay)
			pqParams.DeliveryDelay = &deliveryDelay
		case proto.CPrmPopLimit:
			params, popLimit, err = mpqproto.ParseInt64Param(params, 0, math.MaxInt64)
			pqParams.PopCountLimit = &popLimit
		case proto.CPrmLockTimeout:
			params, lockTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxLockTimeout)
			pqParams.PopLockTimeout = &lockTimeout
		case proto.CPrmFailQueue:
			params, *pqParams.FailQueue, err = mpqproto.ParseItemId(params)
			pqParams.FailQueue = &failQueue
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	return cs.pq.UpdateConfig(pqParams)
}

func (cs *ConnScope) Close() error {
	cs.Finish()
	return cs.responseWriter.Close()
}

func (cs *ConnScope) Finish() {
	atomic.StoreInt32(&cs.stop, 1)
	cs.asyncGroup.Wait()
}
