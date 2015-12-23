package pqueue

import (
	. "firempq/api"
	. "firempq/common"
	. "firempq/conf"
	"firempq/log"
	"fmt"
	"math"
	"sync"
)

type PQContext struct {
	pq             *PQueue
	callsCount     int64
	responseWriter ResponseWriter
	asyncGroup     sync.WaitGroup
	asyncLock      sync.Mutex
	asyncCount     int64
}

func NewPQContext(pq *PQueue, r ResponseWriter) *PQContext {
	return &PQContext{
		pq:             pq,
		callsCount:     0,
		responseWriter: r,
		asyncCount:     512,
	}
}

const MAX_MESSAGE_ID_LENGTH = 128
const MAX_ASYNC_ID = 8
const PAYLOAD_LIMIT = 512 * 1024

const (
	PQ_CMD_UNLOCK_BY_ID        = "UNLCK"
	PQ_CMD_DELETE_LOCKED_BY_ID = "DELLOCKED"
	PQ_CMD_DELETE_BY_ID        = "DEL"
	PQ_CMD_SET_LOCK_TIMEOUT    = "UPDLOCK"
	PQ_CMD_PUSH                = "PUSH"
	PQ_CMD_POP                 = "POP"
	PQ_CMD_POPLOCK             = "POPLCK"
	PQ_CMD_MSG_INFO            = "MSGINFO"
	PQ_CMD_STATUS              = "STATUS"
	PQ_CMD_RELEASE_IN_FLIGHT   = "RELEASE"
	PQ_CMD_EXPIRE              = "EXPIRE"
	PQ_CMD_SET_PARAM           = "SET"
)

const (
	PRM_ID               = "ID"
	PRM_POP_WAIT_TIMEOUT = "WAIT"
	PRM_LOCK_TIMEOUT     = "TIMEOUT"
	PRM_PRIORITY         = "PRIORITY"
	PRM_LIMIT            = "LIMIT"
	PRM_PAYLOAD          = "PL"
	PRM_DELAY            = "DELAY"
	PRM_TIMESTAMP        = "TS"
	PRM_ASYNC            = "ASYNC"
	PRM_SYNC_WAIT        = "SYNCWAIT"
)

const (
	CPRM_MSG_TTL              = "MSGTTL"
	CPRM_MAX_SIZE             = "MAXSIZE"
	CPRM_QUEUE_INACTIVITY_TTL = "QTTL"
	CPRM_DELIVERY_DELAY       = "DELAY"
)

// Call dispatches to the command handler to process necessary parameters.
func (ctx *PQContext) Call(cmd string, params []string) IResponse {
	ctx.callsCount += 1
	switch cmd {
	case PQ_CMD_POPLOCK:
		return ctx.PopLock(params)
	case PQ_CMD_POP:
		return ctx.Pop(params)
	case PQ_CMD_MSG_INFO:
		return ctx.GetMessageInfo(params)
	case PQ_CMD_DELETE_LOCKED_BY_ID:
		return ctx.DeleteLockedById(params)
	case PQ_CMD_DELETE_BY_ID:
		return ctx.DeleteById(params)
	case PQ_CMD_PUSH:
		return ctx.Push(params)
	case PQ_CMD_SET_LOCK_TIMEOUT:
		return ctx.UpdateLock(params)
	case PQ_CMD_UNLOCK_BY_ID:
		return ctx.UnlockMessageById(params)
	case PQ_CMD_STATUS:
		return ctx.GetCurrentStatus(params)
	case PQ_CMD_SET_PARAM:
		return ctx.SetParamValue(params)
	case PQ_CMD_RELEASE_IN_FLIGHT:
		return ctx.ReleaseInFlight(params)
	case PQ_CMD_EXPIRE:
		return ctx.ExpireItems(params)
	}
	return InvalidRequest("Unknown command: " + cmd)
}

// parseMessageIdOnly is looking for message id only.
func parseMessageIdOnly(params []string) (string, *ErrorResponse) {
	var err *ErrorResponse
	var msgId string

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = ParseItemId(params, 1, MAX_MESSAGE_ID_LENGTH)
		default:
			return "", makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return "", err
		}
	}

	if len(msgId) == 0 {
		return "", ERR_MSG_ID_NOT_DEFINED
	}
	return msgId, nil
}

func makeUnknownParamResponse(paramName string) *ErrorResponse {
	return InvalidRequest(fmt.Sprintf("Unknown parameter: %s", paramName))
}

// PopLock gets message from the queue setting lock timeout.
func (ctx *PQContext) PopLock(params []string) IResponse {
	var err *ErrorResponse
	var limit int64 = 1
	var popWaitTimeout int64 = 0
	var lockTimeout int64 = ctx.pq.config.PopLockTimeout
	var asyncId string

	for len(params) > 0 {
		switch params[0] {
		case PRM_LOCK_TIMEOUT:
			params, lockTimeout, err = ParseInt64Param(params, 0, CFG_PQ.MaxLockTimeout)
		case PRM_LIMIT:
			params, limit, err = ParseInt64Param(params, 1, CFG_PQ.MaxPopBatchSize)
		case PRM_POP_WAIT_TIMEOUT:
			params, popWaitTimeout, err = ParseInt64Param(params, 0, CFG_PQ.MaxPopWaitTimeout)
		case PRM_ASYNC:
			params, asyncId, err = ParseItemId(params, 1, MAX_ASYNC_ID)
		default:
			return makeUnknownParamResponse(params[0])
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
		case PRM_POP_WAIT_TIMEOUT:
			params, popWaitTimeout, err = ParseInt64Param(params, 0, CFG_PQ.MaxPopWaitTimeout)
		case PRM_ASYNC:
			params, asyncId, err = ParseItemId(params, 1, MAX_ASYNC_ID)
		default:
			return makeUnknownParamResponse(params[0])
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
		return ERR_ASYNC_WAIT
	}
	go func() {
		ctx.asyncGroup.Add(1)
		defer ctx.asyncGroup.Done()
		res := ctx.pq.Pop(lockTimeout, popWaitTimeout, limit, lock)
		resp := NewAsyncResponse(asyncId, res)
		if err := ctx.responseWriter.WriteResponse(resp); err != nil {
			log.LogConnError(err)
		}
	}()
	return NewStrResponse("A " + asyncId)
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

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (ctx *PQContext) Push(params []string) IResponse {
	cfg := ctx.pq.config
	var err *ErrorResponse
	var msgId string
	var msgTtl int64 = cfg.MsgTtl
	var priority int64 = cfg.MaxPriority - 1
	var delay int64 = cfg.DeliveryDelay
	var payload string = ""
	var syncWait bool
	var asyncId string

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = ParseUserItemId(params, 1, MAX_MESSAGE_ID_LENGTH)
		case PRM_PRIORITY:
			params, priority, err = ParseInt64Param(params, 0, cfg.MaxPriority-1)
		case PRM_PAYLOAD:
			params, payload, err = ParseStringParam(params, 1, PAYLOAD_LIMIT)
		case PRM_DELAY:
			params, delay, err = ParseInt64Param(params, 0, CFG_PQ.MaxDeliveryDelay)
		case PRM_SYNC_WAIT:
			params = params[1:]
			syncWait = true
		case PRM_ASYNC:
			params, asyncId, err = ParseItemId(params, 1, MAX_ASYNC_ID)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return err
		}
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
			return NewStrResponse("A " + asyncId)
		}
	}
	if len(asyncId) > 0 {
		return ERR_ASYNC_PUSH
	}
	return ctx.pq.Push(msgId, payload, msgTtl, delay, priority)
}

// UpdateLock sets a user defined message lock timeout.
// It works only for locked messages.
func (ctx *PQContext) UpdateLock(params []string) IResponse {
	var err *ErrorResponse
	var msgId string
	var lockTimeout int64 = -1

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = ParseItemId(params, 1, MAX_MESSAGE_ID_LENGTH)
		case PRM_LOCK_TIMEOUT:
			params, lockTimeout, err = ParseInt64Param(params, 0, CFG_PQ.MaxLockTimeout)
		default:
			return makeUnknownParamResponse(params[0])
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
	return ctx.pq.UpdateLock(msgId, lockTimeout)
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

func (ctx *PQContext) funcItems(params []string, f func(int64) IResponse) IResponse {
	var err *ErrorResponse
	var ts int64 = -1
	for len(params) > 0 {
		switch params[0] {
		case PRM_TIMESTAMP:
			params, ts, err = ParseInt64Param(params, 0, math.MaxInt64)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return err
		}
	}
	if ts < 0 {
		return ERR_TS_PARAMETER_NEEDED
	}
	return f(ts)
}

func (ctx *PQContext) ReleaseInFlight(params []string) IResponse {
	return ctx.funcItems(params, ctx.pq.ReleaseInFlight)
}

func (ctx *PQContext) ExpireItems(params []string) IResponse {
	return ctx.funcItems(params, ctx.pq.ExpireItems)
}

func (ctx *PQContext) SetParamValue(params []string) IResponse {
	var err *ErrorResponse
	cfg := ctx.pq.config
	msgTtl := cfg.MsgTtl
	maxSize := cfg.MaxSize
	queueTtl := cfg.InactivityTtl
	deliveryDelay := cfg.DeliveryDelay

	for len(params) > 0 {
		switch params[0] {
		case CPRM_MSG_TTL:
			params, msgTtl, err = ParseInt64Param(params, 1, CFG_PQ.MaxMessageTtl)
		case CPRM_MAX_SIZE:
			params, maxSize, err = ParseInt64Param(params, 0, CFG_PQ.MaxLockTimeout)
		case CPRM_DELIVERY_DELAY:
			params, deliveryDelay, err = ParseInt64Param(params, 0, CFG_PQ.MaxDeliveryDelay)
		case CPRM_QUEUE_INACTIVITY_TTL:
			params, deliveryDelay, err = ParseInt64Param(params, 0, math.MaxInt64)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return err
		}
	}
	return ctx.pq.SetParams(msgTtl, maxSize, queueTtl, deliveryDelay)
}

func (ctx *PQContext) Finish() {
	ctx.asyncGroup.Wait()
}
