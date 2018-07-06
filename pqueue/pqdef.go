package pqueue

const (
	StatusQueueMaxSize        = "MaxMsgsInQueue"
	StatusQueuePopWaitTimeout = "PopWaitTimeout"
	StatusQueueMsgTTL         = "MsgTtl"
	StatusQueueDeliveryDelay  = "DeliveryDelay"
	StatusQueuePopLockTimeout = "PopLockTimeout"
	StatusQueuePopCountLimit  = "PopCountLimit"
	StatusQueueCreateTs       = "CreateTs"
	StatusQueueLastPushTs     = "LastPushTs"
	StatusQueueLastPopTs      = "LastPopTs"
	StatusQueueTotalMsgs      = "TotalMessages"
	StatusQueueInFlightMsgs   = "InFlightMessages"
	StatusQueueAvailableMsgs  = "AvailableMessages"
	PQ_STATUS_DELAYED         = "DelayedMessages"
	StatusQueueDeadMsgQueue   = "FailQueue"
	StatusQueueMaxMsgSize     = "MaxMsgSize"
)
