package svcerr

import "strconv"

var CODE_INVALID_REQ int64 = 400

type ServerError struct {
	ErrorText string
	ErrorCode int64
}

func NewError(errorText string, errorCode int64) *ServerError {
	return &ServerError{ErrorText: errorText, ErrorCode: errorCode}
}

func InvalidRequest(errorText string) *ServerError {
	return &ServerError{ErrorText: errorText, ErrorCode: CODE_INVALID_REQ}
}

func (e *ServerError) Error() string {
	return e.ErrorText
}

func (e *ServerError) GetResponse() string {
	return "-ERR:" + strconv.FormatInt(e.ErrorCode, 10) + ":" + e.ErrorText
}

var DISCONNECT = &ServerError{"Disconnect requested", 200}

var ERR_LARGE_REQ = &ServerError{"Too large request!", CODE_INVALID_REQ}
var ERR_UNKNOW_CMD = &ServerError{"Unknown CMD", CODE_INVALID_REQ}

var ERR_NO_SVC = &ServerError{"Service is not created", CODE_INVALID_REQ}
var ERR_SVC_UNKNOWN_TYPE = &ServerError{"Unknown queue type", CODE_INVALID_REQ}
var ERR_SVC_ALREADY_EXISTS = &ServerError{"Queue exists already", CODE_INVALID_REQ}
var ERR_QUEUE_EMPTY = &ServerError{"Queue is empty", CODE_INVALID_REQ}
var ERR_ITEM_ALREADY_EXISTS = &ServerError{"Message exists already", CODE_INVALID_REQ}
var ERR_UNEXPECTED_PRIORITY = &ServerError{"Incrorrect priority", CODE_INVALID_REQ}
var ERR_MSG_NOT_LOCKED = &ServerError{"Message is not locked", CODE_INVALID_REQ}
var ERR_MSG_NOT_EXIST = &ServerError{"Message doesn't exist", CODE_INVALID_REQ}
var ERR_MSG_NOT_DELIVERED = &ServerError{"Message doesn't delivered to the queue", CODE_INVALID_REQ}
var ERR_MSG_IS_LOCKED = &ServerError{"Message is locked", CODE_INVALID_REQ}
var ERR_MSG_POP_ATTEMPTS_EXCEEDED = &ServerError{"Message is locked", CODE_INVALID_REQ}
var ERR_QUEUE_INTERNAL_ERROR = &ServerError{"Internal error/data integrity failure", CODE_INVALID_REQ}

// Param errors
var ERR_MSG_ID_NOT_DEFINED = &ServerError{"Message ID is not defined", CODE_INVALID_REQ}
var ERR_MSG_TIMEOUT_NOT_DEFINED = &ServerError{"Message timeout is not defined", CODE_INVALID_REQ}
var ERR_MSG_ID_TOO_LARGE = &ServerError{"Message ID is limited to 64 symbols", CODE_INVALID_REQ}
var ERR_MSG_NO_PRIORITY = &ServerError{"Message has not prirority", CODE_INVALID_REQ}
var ERR_MSG_WRONG_PRIORITY = &ServerError{"Message priority is incorrect", CODE_INVALID_REQ}
var ERR_MSG_DELIVERY_INTERVAL_NOT_DEFINED = &ServerError{"Delivery interval not defined", CODE_INVALID_REQ}
var ERR_MSG_BAD_DELIVERY_TIMEOUT = &ServerError{"Bad delivery interval specified", CODE_INVALID_REQ}

// TODO: Include allowed time out limits.
var ERR_MSG_TIMEOUT_IS_WRONG = &ServerError{"Message timeout value is wrong.", CODE_INVALID_REQ}
var ERR_MSG_LIMIT_IS_WRONG = &ServerError{"Message limit value is wrong.", CODE_INVALID_REQ}

var ERR_CMD_WITH_NO_PARAMS = &ServerError{"Command doesn't accept any parameters", CODE_INVALID_REQ}
