package dsqueue

import (
	"firempq/util"
)

const (
	DEFAULT_TTL             = 10000 // In milliseconds
	DEFAULT_DELIVERY_DELAY  = 0
	DEFAULT_LOCK_TIMEOUT    = 1000 // In milliseconds
	DEFAULT_POP_COUNT_LIMIT = 0 // 0 means Unlimited.
)

type DSQueueSettings struct {
	MsgTTL         int64 // Default message TTL.
	DeliveryDelay  int64 // Default delivery delay.
	PopLockTimeout int64 // Timeout before message is getting released.
	PopCountLimit  int64 // Pop count limit. 0 - unlimited. >0 Will be removed after this number of attempts.
	MaxSize        int64 // Max queue size.
	MaxPriority    int64 // Max priority.
	CreateTs       int64 // Time when queue was created.
}

type DSQueueStats struct {
	CreateTs        int64 // Time when queue was created.
	LastPushTs      int64 // Last time item has been pushed into queue.
	LastPopTs       int64 // Last pop time.
	LastPushFrontTs int64 // Last time item has been pushed front into queue.
	LastPopFrontTs  int64 // Last front pop time.
	LastPushBackTs int64  // Last time item has been pushed back into queue.
	LastPopBackTs  int64  // Last back pop time.
}

func NewDSQueueSettings(size int64) *DSQueueSettings {
	return &DSQueueSettings{
		MsgTTL:         int64(DEFAULT_TTL),
		DeliveryDelay:  int64(DEFAULT_DELIVERY_DELAY),
		PopLockTimeout: int64(DEFAULT_LOCK_TIMEOUT),
		PopCountLimit:  int64(DEFAULT_POP_COUNT_LIMIT),
		MaxSize:        size,
		CreateTs:       util.Uts(),
	}
}

func (p *DSQueueSettings) ToMap() map[string]interface{} {
	res := make(map[string]interface{})
	res["MaxPriority"] = p.MaxPriority
	res["MaxSize"] = p.MaxSize
	res["CreateTs"] = p.CreateTs
	res["PopTimeOut"] = p.PopLockTimeout
	res["DeliveryDelay"] = p.DeliveryDelay
	res["PopCountLimit"] = p.PopCountLimit
	res["MsgTTL"] = p.MsgTTL
	return res
}
