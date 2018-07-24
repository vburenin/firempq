package proto

const (
	CmdPing        = "PING"
	CmdCreateQueue = "CRT"
	CmdDropQueue   = "DROP"
	CmdQuit        = "QUIT"
	CmdUnitTs      = "TS"
	CmdList        = "LIST"
	CmdCtx         = "CTX"
	CmdPanic       = "PANIC"
	CmdNoCtx       = "NOCTX"
)

const (
	CmdDeleteLockedByID = "DELLCK"
	CmdDeleteByID       = "DEL"
	CmdDeleteByRcpt     = "RDEL"
	CmdUnlockByRcpt     = "RUNLCK"
	CmdUnlockByID       = "UNLCK"
	CmdUpdateLockByID   = "UPDLCK"
	CmdUpdateLockByRcpt = "RUPDLCK"
	CmdPush             = "PUSH"
	CmdPushBatch        = "PUSHB"
	CmdBatchNext        = "NXT"
	CmdPop              = "POP"
	CmdPopLock          = "POPLCK"
	CmdMsgInfo          = "MSGINFO"
	CmdStats            = "STATUS"
	CmdCheckTimeouts    = "CHKTS"
	CmdSetConfig        = "SETCFG"
	CmdPurge            = "PURGE"
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
	CPrmMsgTtl        = "MSGTTL"
	CPrmMaxMsgSize    = "MSGSIZE"
	CPrmMaxQueueSize  = "MAXMSGS"
	CPrmDeliveryDelay = "DELAY"
	CPrmPopLimit      = "POPLIMIT"
	CPrmLockTimeout   = "TIMEOUT"
	CPrmFailQueue     = "FAILQ"
	CPrmPopWait       = "WAIT"
)

const (
	MsgAttrUnlockTs = "UTS"
	MsgAttrExpireTs = "ETS"
	MsgAttrPopCount = "POPCNT"
	MsgAttrRcpt     = "RCPT"
	MsgAttrPayload  = "PL"
	MsgAttrID       = "ID"
)
