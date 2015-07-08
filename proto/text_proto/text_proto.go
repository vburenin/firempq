package text_proto

const (
	CMD_PING         = "PING"
	CMD_PUSH_MSG     = "PUSH"
	CMD_GET_MSG      = "GETM"
	CMD_POP_MSG      = "POPM"
	CMD_CREATE_QUEUE = "CRTQ"
	CMD_DROP_QUEUE   = "DROPPQ"
	CMD_QUEUE_STATS  = "QSTATS"
	CMD_QUIT         = "QUIT"
	CMD_UNIX_TS      = "TS"
)

var RESP_ERROR = "ERR"
var RESP_PONG = "PONG"
var RESP_OK = "OK"
var RESP_BYE = "BYE"

// 384 kb
const MAX_REQ_LENGTH = 393216
