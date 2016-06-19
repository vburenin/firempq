package pqueue

const (
	PQ_STATUS_MAX_QUEUE_SIZE   = "MaxMsgsInQueue"
	PQ_STATUS_POP_WAIT_TIMEOUT = "PopWaitTimeout"
	PQ_STATUS_MSG_TTL          = "MsgTtl"
	PQ_STATUS_DELIVERY_DELAY   = "DeliveryDelay"
	PQ_STATUS_POP_LOCK_TIMEOUT = "PopLockTimeout"
	PQ_STATUS_POP_COUNT_LIMIT  = "PopCountLimit"
	PQ_STATUS_CREATE_TS        = "CreateTs"
	PQ_STATUS_LAST_PUSH_TS     = "LastPushTs"
	PQ_STATUS_LAST_POP_TS      = "LastPopTs"
	PQ_STATUS_TOTAL_MSGS       = "TotalMessages"
	PQ_STATUS_IN_FLIGHT_MSG    = "InFlightMessages"
	PQ_STATUS_AVAILABLE_MSGS   = "AvailableMessages"
	PQ_STATUS_DELAYED          = "DelayedMessages"
	PQ_STATUS_FAIL_QUEUE       = "FailQueue"
	PQ_STATUS_MAX_MSG_SIZE     = "MaxMsgSize"
)
