package pqueue

import (
	"math"
	"sync"

	"firempq/log"

	. "firempq/api"
	. "firempq/common"
	. "firempq/conf"
	. "firempq/errors"
	. "firempq/parsers"
	. "firempq/response"
	. "firempq/services/pqueue/pqmsg"
	. "firempq/services/svcmetadata"
	. "firempq/utils"
)

type PQContext struct {
	pq             *PQueue
	idGen          *IdGen
	callsCount     int64
	responseWriter ResponseWriter
	asyncGroup     sync.WaitGroup
	asyncLock      sync.Mutex
	asyncCount     int64
	finishFlag     bool
}

func NewPQContext(pq *PQueue, r ResponseWriter) *PQContext {
	return &PQContext{
		pq:             pq,
		callsCount:     0,
		responseWriter: r,
		asyncCount:     512,
		idGen:          NewIdGen(),
	}
}

const PAYLOAD_LIMIT = 512 * 1024

const (
	PQ_CMD_DELETE_LOCKED_BY_ID = "DELLCK"
	PQ_CMD_DELETE_BY_ID        = "DEL"
	PQ_CMD_DELETE_BY_RCPT      = "RDEL"
	PQ_CMD_UNLOCK_BY_RCPT      = "RUNLCK"
	PQ_CMD_UNLOCK_BY_ID        = "UNLCK"
	PQ_CMD_UPD_LOCK_BY_ID      = "UPDLCK"
	PQ_CMD_UPD_LOCK_BY_RCPT    = "RUPDLCK"
	PQ_CMD_PUSH                = "PUSH"
	PQ_CMD_POP                 = "POP"
	PQ_CMD_POPLOCK             = "POPLCK"
	PQ_CMD_MSG_INFO            = "MSGINFO"
	PQ_CMD_STATUS              = "STATUS"
	PQ_CMD_CHECK_TIMEOUTS      = "CHKTS"
	PQ_CMD_SET_CFG             = "SETCFG"
)

const (
	PRM_ID           = "ID"
	PRM_RECEIPT      = "RCPT"
	PRM_POP_WAIT     = "WAIT"
	PRM_LOCK_TIMEOUT = "TIMEOUT"
	PRM_PRIORITY     = "PRIORITY"
	PRM_LIMIT        = "LIMIT"
	PRM_PAYLOAD      = "PL"
	PRM_DELAY        = "DELAY"
	PRM_TIMESTAMP    = "TS"
	PRM_ASYNC        = "ASYNC"
	PRM_SYNC_WAIT    = "SYNCWAIT"
	PRM_MSG_TTL      = "TTL"
)

const (
	CPRM_MSG_TTL        = "MSGTTL"
	CPRM_MAX_SIZE       = "MAXSIZE"
	CPRM_DELIVERY_DELAY = "DELAY"
	CPRM_POP_LIMIT      = "POPLIMIT"
	CPRM_LOCK_TIMEOUT   = "TIMEOUT"
	CPRM_FAIL_QUEUE     = "FAILQ"
)

func CreatePQueue(svcs IServices, desc *ServiceDescription, params []string) (ISvc, IResponse) {
	config, resp := ParsePQConfig(params)
	if resp.IsError() {
		return nil, resp
	}
	return InitPQueue(svcs, desc, config), OK_RESPONSE
}

func ParsePQConfig(params []string) (*PQConfig, IResponse) {
	var err *ErrorResponse

	msgTtl := CFG_PQ.DefaultMessageTtl
	maxSize := CFG_PQ.DefaultMaxSize
	delay := CFG_PQ.DefaultDeliveryDelay
	lockTimeout := CFG_PQ.DefaultLockTimeout
	popLimit := CFG_PQ.DefaultPopCountLimit
	failQueue := ""

	for len(params) > 0 {
		switch params[0] {
		case CPRM_MSG_TTL:
			params, msgTtl, err = ParseInt64Param(params, 0, CFG_PQ.MaxMessageTtl)
		case CPRM_MAX_SIZE:
			params, maxSize, err = ParseInt64Param(params, 0, math.MaxInt64)
		case CPRM_DELIVERY_DELAY:
			params, delay, err = ParseInt64Param(params, 0, CFG_PQ.MaxDeliveryDelay)
		case CPRM_POP_LIMIT:
			params, popLimit, err = ParseInt64Param(params, 0, math.MaxInt64)
		case CPRM_LOCK_TIMEOUT:
			params, lockTimeout, err = ParseInt64Param(params, 0, CFG_PQ.MaxLockTimeout)
		case CPRM_FAIL_QUEUE:
			params, failQueue, err = ParseItemId(params)
		default:
			return nil, UnknownParam(params[0])
		}
		if err != nil {
			return nil, err
		}
	}

	cfg := &PQConfig{
		MaxSize:           maxSize,
		MsgTtl:            msgTtl,
		DeliveryDelay:     delay,
		PopLockTimeout:    lockTimeout,
		PopCountLimit:     popLimit,
		LastPushTs:        Uts(),
		LastPopTs:         Uts(),
		PopLimitQueueName: failQueue,
	}

	return cfg, OK_RESPONSE
}

// Call dispatches to the command handler to process necessary parameters.
func (ctx *PQContext) Call(cmd string, params []string) IResponse {
	if ctx.finishFlag {
		return ERR_CONN_CLOSING
	}
	ctx.callsCount += 1
	switch cmd {
	case PQ_CMD_POPLOCK:
		return ctx.PopLock(params)
	case PQ_CMD_POP:
		return ctx.Pop(params)
	case PQ_CMD_MSG_INFO:
		return ctx.GetMessageInfo(params)
	case PQ_CMD_DELETE_BY_RCPT:
		return ctx.DeleteByReceipt(params)
	case PQ_CMD_UNLOCK_BY_RCPT:
		return ctx.UnlockByReceipt(params)
	case PQ_CMD_DELETE_LOCKED_BY_ID:
		return ctx.DeleteLockedById(params)
	case PQ_CMD_DELETE_BY_ID:
		return ctx.DeleteById(params)
	case PQ_CMD_PUSH:
		return ctx.Push(params)
	case PQ_CMD_UPD_LOCK_BY_ID:
		return ctx.UpdateLockById(params)
	case PQ_CMD_UPD_LOCK_BY_RCPT:
		return ctx.UpdateLockByRcpt(params)
	case PQ_CMD_UNLOCK_BY_ID:
		return ctx.UnlockMessageById(params)
	case PQ_CMD_STATUS:
		return ctx.GetCurrentStatus(params)
	case PQ_CMD_SET_CFG:
		return ctx.SetParamValue(params)
	case PQ_CMD_CHECK_TIMEOUTS:
		return ctx.CheckTimeouts(params)
	}
	return InvalidRequest("Unknown command: " + cmd)
}

// parseMessageIdOnly is looking for message id only.
func parseMessageIdOnly(params []string) (string, *ErrorResponse) {
	if len(params) == 1 {
		if ValidateItemId(params[0]) {
			return params[0], nil
		} else {
			return "", ERR_ID_IS_WRONG
		}
	} else if len(params) == 0 {
		return "", ERR_MSG_ID_NOT_DEFINED
	}
	return "", ERR_ONE_ID_ONLY
}

// parseReceiptOnly is looking for message receipt only.
func parseReceiptOnly(params []string) (string, *ErrorResponse) {
	if len(params) == 1 {
		return params[0], nil
	} else if len(params) > 1 {
		return "", ERR_ONE_RECEIPT_ONLY
	}
	return "", ERR_NO_RECEIPT
}

// PopLock gets message from the queue setting lock timeout.
func (ctx *PQContext) PopLock(params []string) IResponse {
	var err *ErrorResponse
	var limit int64 = 1
	var popWaitTimeout int64 = 0
	var asyncId string

	lockTimeout := ctx.pq.config.PopLockTimeout

	for len(params) > 0 {
		switch params[0] {
		case PRM_LOCK_TIMEOUT:
			params, lockTimeout, err = ParseInt64Param(params, 0, CFG_PQ.MaxLockTimeout)
		case PRM_LIMIT:
			params, limit, err = ParseInt64Param(params, 1, CFG_PQ.MaxPopBatchSize)
		case PRM_POP_WAIT:
			params, popWaitTimeout, err = ParseInt64Param(params, 0, CFG_PQ.MaxPopWaitTimeout)
		case PRM_ASYNC:
			params, asyncId, err = ParseItemId(params)
		default:
			return UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	if len(asyncId) > 0 {
		return ctx.asyncPop(asyncId, lockTimeout, popWaitTimeout, limit, true)
	} else {
		return ctx.pq.Pop(lockTimeout, popWaitTimeout, limit, true)
	}
}

// Pop message from queue completely removing it.
func (ctx *PQContext) Pop(params []string) IResponse {
	var err *ErrorResponse
	var limit int64 = 1
	var popWaitTimeout int64 = 0
	var asyncId string

	for len(params) > 0 {
		switch params[0] {
		case PRM_LIMIT:
			params, limit, err = ParseInt64Param(params, 1, CFG_PQ.MaxPopBatchSize)
		case PRM_POP_WAIT:
			params, popWaitTimeout, err = ParseInt64Param(params, 0, CFG_PQ.MaxPopWaitTimeout)
		case PRM_ASYNC:
			params, asyncId, err = ParseItemId(params)
		default:
			return UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	if len(asyncId) > 0 {
		return ctx.asyncPop(asyncId, 0, popWaitTimeout, limit, false)
	} else {
		return ctx.pq.Pop(0, popWaitTimeout, limit, false)
	}
}

func (ctx *PQContext) asyncPop(asyncId string, lockTimeout, popWaitTimeout, limit int64, lock bool) IResponse {
	if len(asyncId) != 0 && popWaitTimeout == 0 {
		return NewAsyncResponse(asyncId, ERR_ASYNC_WAIT)
	}
	go func() {
		ctx.asyncGroup.Add(1)
		res := ctx.pq.Pop(lockTimeout, popWaitTimeout, limit, lock)
		resp := NewAsyncResponse(asyncId, res)
		if err := ctx.responseWriter.WriteResponse(resp); err != nil {
			log.LogConnError(err)
		}
		ctx.asyncGroup.Done()
	}()
	return NewAsyncAccept(asyncId)
}

func (ctx *PQContext) GetMessageInfo(params []string) IResponse {
	msgId, retData := parseMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return ctx.pq.GetMessageInfo(msgId)
}

func (ctx *PQContext) DeleteLockedById(params []string) IResponse {
	msgId, retData := parseMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return ctx.pq.DeleteLockedById(msgId)
}

func (ctx *PQContext) DeleteById(params []string) IResponse {
	msgId, retData := parseMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return ctx.pq.DeleteById(msgId)
}

// DeleteByReceipt deletes locked message using provided receipt.
// This is a preferable method to unlock messages. It helps to avoid
// race condition in case if when message lock has timed out and was
// picked up by other consumer.
func (ctx *PQContext) DeleteByReceipt(params []string) IResponse {
	rcpt, err := parseReceiptOnly(params)
	if err != nil {
		return err
	}
	return ctx.pq.DeleteByReceipt(rcpt)
}

// UnlockByReceipt unlocks locked message using provided receipt.
// Unlocking by receipt is making sure message was not relocked
// by something else.
func (ctx *PQContext) UnlockByReceipt(params []string) IResponse {
	rcpt, err := parseReceiptOnly(params)
	if err != nil {
		return err
	}
	return ctx.pq.UnlockByReceipt(rcpt)
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (ctx *PQContext) Push(params []string) IResponse {
	var err *ErrorResponse
	var msgId string
	var priority int64 = 0
	var syncWait bool
	var asyncId string
	var payload string

	cfg := ctx.pq.config
	delay := cfg.DeliveryDelay
	msgTtl := cfg.MsgTtl

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = ParseUserItemId(params)
		case PRM_PRIORITY:
			params, priority, err = ParseInt64Param(params, 0, math.MaxInt64)
		case PRM_PAYLOAD:
			params, payload, err = ParseStringParam(params, 1, PAYLOAD_LIMIT)
		case PRM_DELAY:
			params, delay, err = ParseInt64Param(params, 0, CFG_PQ.MaxDeliveryDelay)
		case PRM_MSG_TTL:
			params, msgTtl, err = ParseInt64Param(params, 0, CFG_PQ.MaxMessageTtl)
		case PRM_SYNC_WAIT:
			params = params[1:]
			syncWait = true
		case PRM_ASYNC:
			params, asyncId, err = ParseItemId(params)
		default:
			return UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	if len(msgId) == 0 {
		msgId = ctx.idGen.GenRandId()
	}

	if syncWait {
		if len(asyncId) == 0 {
			res := ctx.pq.Push(msgId, payload, msgTtl, delay, priority)
			if !res.IsError() {
				ctx.pq.WaitFlush()
			}
			return res
		} else {
			go func() {
				ctx.asyncGroup.Add(1)
				res := ctx.pq.Push(msgId, payload, msgTtl, delay, priority)
				if !res.IsError() {
					ctx.pq.WaitFlush()
				}
				ctx.responseWriter.WriteResponse(NewAsyncResponse(asyncId, res))
				ctx.asyncGroup.Done()
			}()
			return NewAsyncAccept(asyncId)
		}
	}
	if len(asyncId) > 0 {
		return NewAsyncResponse(asyncId, ERR_ASYNC_PUSH)
	}
	return ctx.pq.Push(msgId, payload, msgTtl, delay, priority)
}

// UpdateLockByRcpt updates message lock according to provided receipt.
func (ctx *PQContext) UpdateLockByRcpt(params []string) IResponse {
	var err *ErrorResponse
	var rcpt string
	var lockTimeout int64 = -1

	for len(params) > 0 {
		switch params[0] {
		case PRM_RECEIPT:
			params, rcpt, err = ParseReceiptParam(params)
		case PRM_LOCK_TIMEOUT:
			params, lockTimeout, err = ParseInt64Param(params, 0, CFG_PQ.MaxLockTimeout)
		default:
			return UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}

	if len(rcpt) == 0 {
		return ERR_NO_RECEIPT
	}

	if lockTimeout < 0 {
		return ERR_MSG_TIMEOUT_NOT_DEFINED
	}
	return ctx.pq.UpdateLockByRcpt(rcpt, lockTimeout)
}

// UpdateLockById sets a user defined message lock timeout.
// It works only for locked messages.
func (ctx *PQContext) UpdateLockById(params []string) IResponse {
	var err *ErrorResponse
	var msgId string
	var lockTimeout int64 = -1

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = ParseItemId(params)
		case PRM_LOCK_TIMEOUT:
			params, lockTimeout, err = ParseInt64Param(params, 0, CFG_PQ.MaxLockTimeout)
		default:
			return UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}

	if len(msgId) == 0 {
		return ERR_MSG_ID_NOT_DEFINED
	}

	if lockTimeout < 0 {
		return ERR_MSG_TIMEOUT_NOT_DEFINED
	}
	return ctx.pq.UpdateLockById(msgId, lockTimeout)
}

func (ctx *PQContext) UnlockMessageById(params []string) IResponse {
	msgId, retData := parseMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return ctx.pq.UnlockMessageById(msgId)
}

func (ctx *PQContext) GetCurrentStatus(params []string) IResponse {
	if len(params) > 0 {
		return ERR_CMD_WITH_NO_PARAMS
	}
	return ctx.pq.GetCurrentStatus()
}

func (ctx *PQContext) CheckTimeouts(params []string) IResponse {
	var err *ErrorResponse
	var ts int64 = -1
	for len(params) > 0 {
		switch params[0] {
		case PRM_TIMESTAMP:
			params, ts, err = ParseInt64Param(params, 0, math.MaxInt64)
		default:
			return UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	if ts < 0 {
		return ERR_TS_PARAMETER_NEEDED
	}
	return ctx.pq.CheckTimeouts(ts)
}

func (ctx *PQContext) SetParamValue(params []string) IResponse {
	var err *ErrorResponse
	cfg := ctx.pq.config
	msgTtl := cfg.MsgTtl
	maxSize := cfg.MaxSize
	popLimit := cfg.PopCountLimit
	deliveryDelay := cfg.DeliveryDelay
	lockTimeout := cfg.PopLockTimeout
	failQueue := ""

	if len(params) == 0 {
		return ERR_CMD_PARAM_NOT_PROVIDED
	}

	for len(params) > 0 {
		switch params[0] {
		case CPRM_MSG_TTL:
			params, msgTtl, err = ParseInt64Param(params, 1, CFG_PQ.MaxMessageTtl)
		case CPRM_MAX_SIZE:
			params, maxSize, err = ParseInt64Param(params, 0, math.MaxInt64)
		case CPRM_DELIVERY_DELAY:
			params, deliveryDelay, err = ParseInt64Param(params, 0, CFG_PQ.MaxDeliveryDelay)
		case CPRM_POP_LIMIT:
			params, popLimit, err = ParseInt64Param(params, 0, math.MaxInt64)
		case CPRM_LOCK_TIMEOUT:
			params, lockTimeout, err = ParseInt64Param(params, 0, CFG_PQ.MaxLockTimeout)
		case CPRM_FAIL_QUEUE:
			params, failQueue, err = ParseItemId(params)
		default:
			return UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	return ctx.pq.SetParams(msgTtl, maxSize, deliveryDelay, popLimit, lockTimeout, failQueue)
}

func (ctx *PQContext) Finish() {
	if !ctx.finishFlag {
		ctx.finishFlag = true
		ctx.asyncGroup.Wait()
	}
}
