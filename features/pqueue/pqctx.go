package pqueue

import (
	. "firempq/api"
	. "firempq/common"
	. "firempq/conf"
	"math"
)

type PQContext struct {
	pq         *PQueue
	callsCount int64
}

const (
	ACTION_UNLOCK_BY_ID        = "UNLOCK"
	ACTION_DELETE_LOCKED_BY_ID = "DELLOCKED"
	ACTION_DELETE_BY_ID        = "DEL"
	ACTION_SET_LOCK_TIMEOUT    = "SETLOCKTIMEOUT"
	ACTION_PUSH                = "PUSH"
	ACTION_POP                 = "POP"
	ACTION_STATUS              = "STATUS"
	ACTION_RELEASE_IN_FLIGHT   = "RELEASE"
	ACTION_EXPIRE              = "EXPIRE"
)

const (
	PRM_ID               = "ID"
	PRM_POP_WAIT_TIMEOUT = "WAITTIMEOUT"
	PRM_LOCK_TIMEOUT     = "LOCKTIMEOUT"
	PRM_PRIORITY         = "PRIORITY"
	PRM_LIMIT            = "LIMIT"
	PRM_PAYLOAD          = "PL"
	PRM_DELAY            = "DELAY"
	PRM_TIMESTAMP        = "TS"
)

// Call dispatches to the command handler to process necessary parameters.
func (ctx *PQContext) Call(cmd string, params []string) IResponse {
	ctx.callsCount += 1
	switch cmd {
	case ACTION_POP:
		return ctx.Pop(params)
	case ACTION_DELETE_LOCKED_BY_ID:
		return ctx.DeleteLockedById(params)
	case ACTION_DELETE_BY_ID:
		return ctx.DeleteById(params)
	case ACTION_PUSH:
		return ctx.Push(params)
	case ACTION_SET_LOCK_TIMEOUT:
		return ctx.SetLockTimeout(params)
	case ACTION_UNLOCK_BY_ID:
		return ctx.UnlockMessageById(params)
	case ACTION_STATUS:
		return ctx.GetCurrentStatus(params)
	case ACTION_RELEASE_IN_FLIGHT:
		return ctx.ReleaseInFlight(params)
	case ACTION_EXPIRE:
		return ctx.ExpireItems(params)
	}
	return InvalidRequest("Unknown action: " + cmd)
}

// parseMessageIdOnly is looking for message id only.
func parseMessageIdOnly(params []string) (string, *ErrorResponse) {
	var err *ErrorResponse
	var msgId string

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = ParseStringParam(params, 1, 128)
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

// Pop get message from the queue.
func (ctx *PQContext) Pop(params []string) IResponse {
	var err *ErrorResponse
	var limit int64 = 1
	var popWaitTimeout int64 = 0
	var lockTimeout int64 = ctx.pq.config.PopLockTimeout

	for len(params) > 0 {
		switch params[0] {
		case PRM_LOCK_TIMEOUT:
			params, lockTimeout, err = ParseInt64Param(params, 0, 24*1000*3600)
		case PRM_LIMIT:
			params, limit, err = ParseInt64Param(params, 1, CFG_PQ.MaxPopBatchSize)
		case PRM_POP_WAIT_TIMEOUT:
			params, popWaitTimeout, err = ParseInt64Param(params, 0, CFG_PQ.MaxPopWaitTimeout)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return err
		}
	}
	return ctx.pq.Pop(lockTimeout, popWaitTimeout, limit)
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
	var err *ErrorResponse
	var msgId string
	var priority int64 = ctx.pq.config.MaxPriority - 1
	var payload string = ""
	var delay int64

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = ParseStringParam(params, 1, 128)
		case PRM_PRIORITY:
			params, priority, err = ParseInt64Param(params, 0, ctx.pq.config.MaxPriority-1)
		case PRM_PAYLOAD:
			params, payload, err = ParseStringParam(params, 1, 512*1024)
		case PRM_DELAY:
			params, delay, err = ParseInt64Param(params, 0, CFG_PQ.MaxDeliveryTimeout)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return err
		}
	}

	return ctx.pq.Push(msgId, payload, delay, priority)
}

// SetLockTimeout sets a user defined message lock timeout.
// It works only for locked messages.
func (ctx *PQContext) SetLockTimeout(params []string) IResponse {
	var err *ErrorResponse
	var msgId string
	var lockTimeout int64 = -1

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = ParseStringParam(params, 1, 128)
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
	return ctx.pq.SetLockTimeout(msgId, lockTimeout)
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
	}
	if err != nil {
		return err
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

func (ctx *PQContext) Finish() {}
