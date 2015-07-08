package pqueue

import (
	"firempq/util"
)

const (
	DEFAULT_TTL             = 12000000
	DEFAULT_DELIVERY_DELAY  = 0
	DEFAULT_LOCK_TIMEOUT    = 30000000
	DEFAULT_POP_COUNT_LIMIT = 0 // 0 means Unlimited.
)

type PQueueSettings struct {
	MsgTTL         int64 // Default message TTL.
	DeliveryDelay  int64 // Default delivery delay.
	PopLockTimeout int64 // Timeout before message is getting released.
	PopCountLimit  int64 // Pop count limit. 0 - unlimited. >0 Will be removed after this number of attempts.
	MaxSize        int64 // Max queue size.
	MaxPriority    int64 // Max priority.
	CreateTs       int64 // Time when queue was created.
	LastPushTs     int64 // Last time item has been pushed into queue.
	LastPopTs      int64 // Last pop time.
}

func NewPQueueSettings(priorities, size int64) *PQueueSettings {
	return &PQueueSettings{
		MsgTTL:         DEFAULT_TTL,
		DeliveryDelay:  DEFAULT_DELIVERY_DELAY,
		PopLockTimeout: DEFAULT_LOCK_TIMEOUT,
		PopCountLimit:  DEFAULT_POP_COUNT_LIMIT,
		MaxSize:        size,
		LastPushTs:     0,
		LastPopTs:      0,
		MaxPriority:    priorities,
		CreateTs:       util.Uts(),
	}
}

func (p *PQueueSettings) ToMap() map[string]interface{} {
	res := make(map[string]interface{})
	res["MaxPriority"] = p.MaxPriority
	res["MaxSize"] = p.MaxSize
	res["CreateTs"] = p.CreateTs
	res["LastPushTs"] = p.LastPushTs
	res["LastPopTs"] = p.LastPopTs
	res["PopTimeOut"] = p.PopLockTimeout
	res["DeliveryDelay"] = p.DeliveryDelay
	res["PopCountLimit"] = p.PopCountLimit
	res["MsgTTL"] = p.MsgTTL
	return res
}
