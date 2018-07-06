package pqueue

import (
	"math"
	"sync"

	"github.com/jessevdk/go-flags"
	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/idgen"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/mpqproto"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/utils"
)

type ConnScope struct {
	pq             *PQueue
	idGen          *idgen.IdGen
	callsCount     int64
	responseWriter apis.ResponseWriter
	asyncGroup     sync.WaitGroup
	asyncLock      sync.Mutex
	asyncCount     int64
	finishFlag     bool
}

func NewConnScope(pq *PQueue, r apis.ResponseWriter) *ConnScope {
	return &ConnScope{
		pq:             pq,
		callsCount:     0,
		responseWriter: r,
		asyncCount:     512,
		idGen:          idgen.NewGen(),
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
	PQ_CMD_PURGE               = "PURGE"
)

const (
	PrmID          = "ID"
	PrmReceipt     = "RCPT"
	PrmPopWait     = "WAIT"
	PrmLockTimeout = "TIMEOUT"
	PrmLimit       = "LIMIT"
	PrmPayload     = "PL"
	PrmDelay       = "DELAY"
	PrmTimeStamp   = "TS"
	PrmAsync       = "ASYNC"
	PrmSyncWait    = "SYNCWAIT"
	PrmMsgTTL      = "TTL"
)

const (
	CPRM_MSG_TTL           = "MSGTTL"
	CPRM_MAX_MSG_SIZE      = "MSGSIZE"
	CPRM_MAX_MSGS_IN_QUEUE = "MAXMSGS"
	CPRM_DELIVERY_DELAY    = "DELAY"
	CPRM_POP_LIMIT         = "POPLIMIT"
	CPRM_LOCK_TIMEOUT      = "TIMEOUT"
	CPRM_FAIL_QUEUE        = "FAILQ"
	CPRM_POP_WAIT          = "WAIT"
)

func DefaultPQConfig() *conf.PQConfig {
	cfg := &conf.Config{}
	flags.ParseArgs(cfg, []string{"firempq"})

	conf.CFG = cfg
	conf.CFG_PQ = &cfg.PQueueConfig

	return &conf.PQConfig{
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

func ParsePQConfig(params []string) (*conf.PQConfig, apis.IResponse) {
	var err *mpqerr.ErrorResponse

	cfg := DefaultPQConfig()
	for len(params) > 0 {
		switch params[0] {
		case CPRM_MSG_TTL:
			params, cfg.MsgTtl, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxMessageTTL)
		case CPRM_MAX_MSG_SIZE:
			params, cfg.MaxMsgSize, err = mpqproto.ParseInt64Param(params, 1024, conf.CFG_PQ.MaxMessageSize)
		case CPRM_MAX_MSGS_IN_QUEUE:
			params, cfg.MaxMsgsInQueue, err = mpqproto.ParseInt64Param(params, 0, math.MaxInt64)
		case CPRM_DELIVERY_DELAY:
			params, cfg.DeliveryDelay, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxDeliveryDelay)
		case CPRM_POP_LIMIT:
			params, cfg.PopCountLimit, err = mpqproto.ParseInt64Param(params, 0, math.MaxInt64)
		case CPRM_LOCK_TIMEOUT:
			params, cfg.PopLockTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxLockTimeout)
		case CPRM_FAIL_QUEUE:
			params, cfg.PopLimitQueueName, err = mpqproto.ParseItemId(params)
		case CPRM_POP_WAIT:
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

// Call dispatches to the command handler to process necessary parameters.
func (ctx *ConnScope) Call(cmd string, params []string) apis.IResponse {
	if ctx.finishFlag {
		return mpqerr.ERR_CONN_CLOSING
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
	case PQ_CMD_PURGE:
		ctx.pq.Clear()
		return resp.OK
	}
	return mpqerr.InvalidRequest("Unknown command: " + cmd)
}

// parseMessageIdOnly is looking for message id only.
func parseMessageIdOnly(params []string) (string, *mpqerr.ErrorResponse) {
	if len(params) == 1 {
		if mpqproto.ValidateItemId(params[0]) {
			return params[0], nil
		} else {
			return "", mpqerr.ERR_ID_IS_WRONG
		}
	} else if len(params) == 0 {
		return "", mpqerr.ERR_MSG_ID_NOT_DEFINED
	}
	return "", mpqerr.ERR_ONE_ID_ONLY
}

// parseReceiptOnly is looking for message receipt only.
func parseReceiptOnly(params []string) (string, *mpqerr.ErrorResponse) {
	if len(params) == 1 {
		return params[0], nil
	} else if len(params) > 1 {
		return "", mpqerr.ERR_ONE_RECEIPT_ONLY
	}
	return "", mpqerr.ERR_NO_RECEIPT
}

// PopLock gets message from the queue setting lock timeout.
func (ctx *ConnScope) PopLock(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var limit int64 = 1
	var asyncId string

	popWaitTimeout := ctx.pq.config.PopWaitTimeout
	lockTimeout := ctx.pq.config.PopLockTimeout

	for len(params) > 0 {
		switch params[0] {
		case PrmLockTimeout:
			params, lockTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxLockTimeout)
		case PrmLimit:
			params, limit, err = mpqproto.ParseInt64Param(params, 1, conf.CFG_PQ.MaxPopBatchSize)
		case PrmPopWait:
			params, popWaitTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxPopWaitTimeout)
		case PrmAsync:
			params, asyncId, err = mpqproto.ParseItemId(params)
		default:
			return mpqerr.UnknownParam(params[0])
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
func (ctx *ConnScope) Pop(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var limit int64 = 1
	var asyncId string

	popWaitTimeout := ctx.pq.config.PopWaitTimeout

	for len(params) > 0 {
		switch params[0] {
		case PrmLimit:
			params, limit, err = mpqproto.ParseInt64Param(params, 1, conf.CFG_PQ.MaxPopBatchSize)
		case PrmPopWait:
			params, popWaitTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxPopWaitTimeout)
		case PrmAsync:
			params, asyncId, err = mpqproto.ParseItemId(params)
		default:
			return mpqerr.UnknownParam(params[0])
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

func (ctx *ConnScope) asyncPop(asyncId string, lockTimeout, popWaitTimeout, limit int64, lock bool) apis.IResponse {
	if len(asyncId) != 0 && popWaitTimeout == 0 {
		return resp.NewAsyncResponse(asyncId, mpqerr.ERR_ASYNC_WAIT)
	}
	go func() {
		ctx.asyncGroup.Add(1)
		res := ctx.pq.Pop(lockTimeout, popWaitTimeout, limit, lock)
		r := resp.NewAsyncResponse(asyncId, res)
		if err := ctx.responseWriter.WriteResponse(r); err != nil {
			log.LogConnError(err)
		}
		ctx.asyncGroup.Done()
	}()
	return resp.NewAsyncAccept(asyncId)
}

func (ctx *ConnScope) GetMessageInfo(params []string) apis.IResponse {
	msgId, retData := parseMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return ctx.pq.GetMessageInfo(msgId)
}

func (ctx *ConnScope) DeleteLockedById(params []string) apis.IResponse {
	msgId, retData := parseMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return ctx.pq.DeleteLockedById(msgId)
}

func (ctx *ConnScope) DeleteById(params []string) apis.IResponse {
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
func (ctx *ConnScope) DeleteByReceipt(params []string) apis.IResponse {
	rcpt, err := parseReceiptOnly(params)
	if err != nil {
		return err
	}
	return ctx.pq.DeleteByReceipt(rcpt)
}

// UnlockByReceipt unlocks locked message using provided receipt.
// Unlocking by receipt is making sure message was not relocked
// by something else.
func (ctx *ConnScope) UnlockByReceipt(params []string) apis.IResponse {
	rcpt, err := parseReceiptOnly(params)
	if err != nil {
		return err
	}
	return ctx.pq.UnlockByReceipt(rcpt)
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (ctx *ConnScope) Push(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var msgId string
	var syncWait bool
	var asyncId string
	var payload string

	cfg := ctx.pq.config
	delay := cfg.DeliveryDelay
	msgTtl := cfg.MsgTtl

	for len(params) > 0 {
		switch params[0] {
		case PrmID:
			params, msgId, err = mpqproto.ParseUserItemId(params)
		case PrmPayload:
			params, payload, err = mpqproto.ParseStringParam(params, 1, PAYLOAD_LIMIT)
		case PrmDelay:
			params, delay, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxDeliveryDelay)
		case PrmMsgTTL:
			params, msgTtl, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxMessageTTL)
		case PrmSyncWait:
			params = params[1:]
			syncWait = true
		case PrmAsync:
			params, asyncId, err = mpqproto.ParseItemId(params)
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	if len(msgId) == 0 {
		msgId = ctx.idGen.RandId()
	}

	if syncWait {
		if len(asyncId) == 0 {
			res := ctx.pq.Push(msgId, payload, msgTtl, delay)
			if !res.IsError() {
				// TODO(vburenin): Add flush wait.
				// ctx.pq.WaitFlush()
			}
			return res
		} else {
			go func() {
				ctx.asyncGroup.Add(1)
				res := ctx.pq.Push(msgId, payload, msgTtl, delay)
				if !res.IsError() {
					// TODO(vburenin): Add flush wait.
					// ctx.pq.WaitFlush()
				}
				ctx.responseWriter.WriteResponse(resp.NewAsyncResponse(asyncId, res))
				ctx.asyncGroup.Done()
			}()
			return resp.NewAsyncAccept(asyncId)
		}
	}
	if len(asyncId) > 0 {
		return resp.NewAsyncResponse(asyncId, mpqerr.ERR_ASYNC_PUSH)
	}
	return ctx.pq.Push(msgId, payload, msgTtl, delay)
}

// UpdateLockByRcpt updates message lock according to provided receipt.
func (ctx *ConnScope) UpdateLockByRcpt(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var rcpt string
	var lockTimeout int64 = -1

	for len(params) > 0 {
		switch params[0] {
		case PrmReceipt:
			params, rcpt, err = mpqproto.ParseReceiptParam(params)
		case PrmLockTimeout:
			params, lockTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxLockTimeout)
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}

	if len(rcpt) == 0 {
		return mpqerr.ERR_NO_RECEIPT
	}

	if lockTimeout < 0 {
		return mpqerr.ERR_MSG_TIMEOUT_NOT_DEFINED
	}
	return ctx.pq.UpdateLockByRcpt(rcpt, lockTimeout)
}

// UpdateLockById sets a user defined message lock timeout.
// It works only for locked messages.
func (ctx *ConnScope) UpdateLockById(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var msgId string
	var lockTimeout int64 = -1

	for len(params) > 0 {
		switch params[0] {
		case PrmID:
			params, msgId, err = mpqproto.ParseItemId(params)
		case PrmLockTimeout:
			params, lockTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxLockTimeout)
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}

	if len(msgId) == 0 {
		return mpqerr.ERR_MSG_ID_NOT_DEFINED
	}

	if lockTimeout < 0 {
		return mpqerr.ERR_MSG_TIMEOUT_NOT_DEFINED
	}
	return ctx.pq.UpdateLockById(msgId, lockTimeout)
}

func (ctx *ConnScope) UnlockMessageById(params []string) apis.IResponse {
	msgId, retData := parseMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	return ctx.pq.UnlockMessageById(msgId)
}

func (ctx *ConnScope) GetCurrentStatus(params []string) apis.IResponse {
	if len(params) > 0 {
		return mpqerr.ERR_CMD_WITH_NO_PARAMS
	}
	return ctx.pq.GetCurrentStatus()
}

func (ctx *ConnScope) CheckTimeouts(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	var ts int64 = -1
	for len(params) > 0 {
		switch params[0] {
		case PrmTimeStamp:
			params, ts, err = mpqproto.ParseInt64Param(params, 0, math.MaxInt64)
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	if ts < 0 {
		return mpqerr.ERR_TS_PARAMETER_NEEDED
	}
	return ctx.pq.CheckTimeouts(ts)
}

func (ctx *ConnScope) SetParamValue(params []string) apis.IResponse {
	var err *mpqerr.ErrorResponse
	cfg := ctx.pq.config
	msgTtl := cfg.MsgTtl
	maxMsgsInQueue := cfg.MaxMsgsInQueue
	maxMsgSize := cfg.MaxMsgSize
	popLimit := cfg.PopCountLimit
	deliveryDelay := cfg.DeliveryDelay
	lockTimeout := cfg.PopLockTimeout
	failQueue := ""

	if len(params) == 0 {
		return mpqerr.ERR_CMD_PARAM_NOT_PROVIDED
	}

	pqParams := &QueueParams{}

	for len(params) > 0 {
		switch params[0] {
		case CPRM_MSG_TTL:
			params, msgTtl, err = mpqproto.ParseInt64Param(params, 1, conf.CFG_PQ.MaxMessageTTL)
			pqParams.MsgTTL = &msgTtl
		case CPRM_MAX_MSG_SIZE:
			params, maxMsgSize, err = mpqproto.ParseInt64Param(params, 1024, conf.CFG_PQ.MaxMessageSize)
			pqParams.MaxMsgSize = &maxMsgSize
		case CPRM_MAX_MSGS_IN_QUEUE:
			params, maxMsgsInQueue, err = mpqproto.ParseInt64Param(params, 0, math.MaxInt64)
			pqParams.MaxMsgsInQueue = &maxMsgsInQueue
		case CPRM_DELIVERY_DELAY:
			params, deliveryDelay, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxDeliveryDelay)
			pqParams.DeliveryDelay = &deliveryDelay
		case CPRM_POP_LIMIT:
			params, popLimit, err = mpqproto.ParseInt64Param(params, 0, math.MaxInt64)
			pqParams.PopCountLimit = &popLimit
		case CPRM_LOCK_TIMEOUT:
			params, lockTimeout, err = mpqproto.ParseInt64Param(params, 0, conf.CFG_PQ.MaxLockTimeout)
			pqParams.PopLockTimeout = &lockTimeout
		case CPRM_FAIL_QUEUE:
			params, *pqParams.FailQueue, err = mpqproto.ParseItemId(params)
			pqParams.FailQueue = &failQueue
		default:
			return mpqerr.UnknownParam(params[0])
		}
		if err != nil {
			return err
		}
	}
	return ctx.pq.SetParams(pqParams)
}

func (ctx *ConnScope) Finish() {
	if !ctx.finishFlag {
		ctx.finishFlag = true
		ctx.asyncGroup.Wait()
	}
}
