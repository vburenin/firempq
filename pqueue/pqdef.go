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
	StatusQueueDeadMsgQueue   = "FailQueue"
	StatusQueueMaxMsgSize     = "MaxMsgSize"
)

const DBActionAddMetadata = byte(1)
const DBActionUpdateMetadata = byte(2)
const DBActionDeleteMetadata = byte(3)
const DBActionWipeAll = byte(4)
