package dsqueue

import (
	. "firempq/api"
	. "firempq/common"
	. "firempq/conf"
	"math"
)

type DSQContext struct {
	dsq        *DSQueue
	callsCount int64
}

const (
	ACTION_UNLOCK_BY_ID        = "UNLOCK"
	ACTION_DELETE_LOCKED_BY_ID = "DELLOCKED"
	ACTION_DELETE_BY_ID        = "DEL"
	ACTION_SET_LOCK_TIMEOUT    = "SETLOCKTIMEOUT"

	ACTION_PUSH_FRONT     = "PUSHFRONT"
	ACTION_RETURN_FRONT   = "RETURNFRONT"
	ACTION_POP_FRONT      = "POPFRONT"
	ACTION_POP_LOCK_FRONT = "POPLOCK"

	ACTION_PUSH_BACK     = "PUSHBACK"
	ACTION_RETURN_BACK   = "RETURNBACK"
	ACTION_POP_BACK      = "POPBACK"
	ACTION_POP_LOCK_BACK = "POPLOCKBACK"

	ACTION_FORCE_DELETE_BY_ID = "FORCEDELETE"
	ACTION_STATUS             = "STATUS"
	ACTION_RELEASE_IN_FLIGHT  = "RELEASE"
	ACTION_EXPIRE             = "EXPIRE"
)

const (
	PRM_ID               = "ID"
	PRM_POP_WAIT_TIMEOUT = "WAITTIMEOUT"
	PRM_LOCK_TIMEOUT     = "LOCKTIMEOUT"
	PRM_LIMIT            = "LIMIT"
	PRM_PAYLOAD          = "PL"
	PRM_DELAY            = "DELAY"
	PRM_TIMESTAMP        = "TS"
)

// Call dispatches to the command handler to process necessary parameters.
func (ctx *DSQContext) Call(cmd string, params []string) IResponse {
	ctx.callsCount += 1
	switch cmd {
	case ACTION_DELETE_LOCKED_BY_ID:
		return ctx.DeleteLockedById(params)
	case ACTION_DELETE_BY_ID:
		return ctx.DeleteById(params)
	case ACTION_FORCE_DELETE_BY_ID:
		return ctx.ForceDeleteById(params)
	case ACTION_SET_LOCK_TIMEOUT:
		return ctx.SetLockTimeout(params)
	case ACTION_UNLOCK_BY_ID:
		return ctx.UnlockMessageById(params)
	case ACTION_PUSH_FRONT:
		return ctx.PushFront(params)
	case ACTION_POP_LOCK_FRONT:
		return ctx.PopLockFront(params)
	case ACTION_POP_FRONT:
		return ctx.PopFront(params)
	case ACTION_RETURN_FRONT:
		return ctx.ReturnFront(params)
	case ACTION_PUSH_BACK:
		return ctx.PushBack(params)
	case ACTION_POP_LOCK_BACK:
		return ctx.PopLockBack(params)
	case ACTION_POP_BACK:
		return ctx.PopBack(params)
	case ACTION_RETURN_BACK:
		return ctx.ReturnBack(params)
	case ACTION_STATUS:
		return ctx.GetCurrentStatus(params)
	case ACTION_RELEASE_IN_FLIGHT:
		return ctx.ReleaseInFlight(params)
	case ACTION_EXPIRE:
		return ctx.ExpireItems(params)
	}
	return InvalidRequest("Unknown action: " + cmd)
}

func getMessageIdOnly(params []string) (string, *ErrorResponse) {
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

func (ctx *DSQContext) DeleteLockedById(params []string) IResponse {
	msgId, retData := getMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return ctx.dsq.DeleteLockedById(msgId)
}

func (ctx *DSQContext) DeleteById(params []string) IResponse {
	msgId, retData := getMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return ctx.dsq.DeleteById(msgId)
}

func (ctx *DSQContext) ForceDeleteById(params []string) IResponse {
	msgId, retData := getMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return ctx.dsq.ForceDeleteById(msgId)
}

func (ctx *DSQContext) SetLockTimeout(params []string) IResponse {
	var err *ErrorResponse
	var msgId string
	var lockTimeout int64 = -1

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = ParseStringParam(params, 1, 128)
		case PRM_LOCK_TIMEOUT:
			params, lockTimeout, err = ParseInt64Param(params, 0, 24*1000*3600)
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
	return ctx.dsq.SetLockTimeout(msgId, lockTimeout)
}

func (ctx *DSQContext) UnlockMessageById(params []string) IResponse {
	msgId, retData := getMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return ctx.dsq.UnlockMessageById(msgId)
}

func (ctx *DSQContext) push(params []string, direction int32) IResponse {
	var err *ErrorResponse
	var msgId string
	var payload string
	var deliveryDelay = ctx.dsq.conf.DeliveryDelay

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = ParseStringParam(params, 1, 128)
		case PRM_PAYLOAD:
			params, payload, err = ParseStringParam(params, 1, 512*1024)
		case PRM_DELAY:
			params, deliveryDelay, err = ParseInt64Param(params, 0, 3600*1000)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return err
		}
	}
	if len(msgId) == 0 {
		msgId = GenRandMsgId()
	}

	if deliveryDelay < 0 || deliveryDelay > CFG_DSQ.MaxDeliveryTimeout || ctx.dsq.conf.MsgTtl < deliveryDelay {
		return ERR_MSG_BAD_DELIVERY_TIMEOUT
	}
	return ctx.dsq.Push(msgId, payload, deliveryDelay, direction)
}

func (ctx *DSQContext) PushFront(params []string) IResponse {
	return ctx.push(params, QUEUE_DIRECTION_FRONT)
}

func (ctx *DSQContext) PushBack(params []string) IResponse {
	return ctx.push(params, QUEUE_DIRECTION_BACK)
}

// PopLockFront pops first available message in the front removing message fron the queue.
func (ctx *DSQContext) PopLockFront(params []string) IResponse {
	return ctx.dsq.popMessage(QUEUE_DIRECTION_FRONT, false)
}

func (ctx *DSQContext) PopLockBack(params []string) IResponse {
	return ctx.dsq.popMessage(QUEUE_DIRECTION_BACK, false)
}

func (ctx *DSQContext) PopFront(params []string) IResponse {
	return ctx.dsq.popMessage(QUEUE_DIRECTION_FRONT, true)
}

func (ctx *DSQContext) PopBack(params []string) IResponse {
	return ctx.dsq.popMessage(QUEUE_DIRECTION_BACK, true)
}

func (ctx *DSQContext) ReturnFront(params []string) IResponse {
	return ctx.dsq.returnMessageTo(params, QUEUE_DIRECTION_FRONT)
}

func (ctx *DSQContext) ReturnBack(params []string) IResponse {
	return ctx.dsq.returnMessageTo(params, QUEUE_DIRECTION_BACK)
}

func (ctx *DSQContext) GetCurrentStatus(params []string) IResponse {
	if len(params) > 0 {
		return ERR_CMD_WITH_NO_PARAMS
	}
	return ctx.dsq.GetCurrentStatus()
}

func (ctx *DSQContext) funcItems(params []string, f func(int64) int64) IResponse {
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
	return NewIntResponse(f(ts))
}

func (ctx *DSQContext) ReleaseInFlight(params []string) IResponse {
	return ctx.funcItems(params, ctx.dsq.releaseInFlight)
}

func (ctx *DSQContext) ExpireItems(params []string) IResponse {
	return ctx.funcItems(params, ctx.dsq.cleanExpiredItems)
}

func (ctx *DSQContext) Finish() {}
