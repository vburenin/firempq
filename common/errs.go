package common

var CODE_INVALID_REQ int64 = 400
var CODE_CONFLICT_REQ int64 = 409
var CODE_SERVER_ERR int64 = 500

func NewError(errorText string, errorCode int64) *ErrorResponse {
	return &ErrorResponse{errorText, errorCode}
}

func InvalidRequest(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CODE_INVALID_REQ}
}

func ConflictRequest(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CODE_CONFLICT_REQ}
}

func ServerSideError(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CODE_SERVER_ERR}
}

var ERR_UNKNOWN_CMD *ErrorResponse = InvalidRequest("Unknown CMD")

var ERR_NO_SVC *ErrorResponse = InvalidRequest("Service is not created")
var ERR_SVC_UNKNOWN_TYPE *ErrorResponse = InvalidRequest("Unknown service type")
var ERR_SVC_ALREADY_EXISTS *ErrorResponse = InvalidRequest("Service exists already")
var ERR_ITEM_ALREADY_EXISTS *ErrorResponse = InvalidRequest("Message exists already")
var ERR_UNEXPECTED_PRIORITY *ErrorResponse = InvalidRequest("Incrorrect priority")
var ERR_MSG_NOT_LOCKED *ErrorResponse = InvalidRequest("Message is not locked")
var ERR_MSG_NOT_EXIST *ErrorResponse = InvalidRequest("Message doesn't exist")
var ERR_MSG_NOT_DELIVERED *ErrorResponse = InvalidRequest("Message isn't delivered to the queue")
var ERR_MSG_IS_LOCKED *ErrorResponse = InvalidRequest("Message is locked")
var ERR_MSG_POP_ATTEMPTS_EXCEEDED *ErrorResponse = InvalidRequest("Message is locked")
var ERR_QUEUE_INTERNAL_ERROR *ErrorResponse = InvalidRequest("Internal error/data integrity failure")

// Param errors
var ERR_MSG_ID_NOT_DEFINED *ErrorResponse = InvalidRequest("Message ID is not defined")
var ERR_MSG_TIMEOUT_NOT_DEFINED *ErrorResponse = InvalidRequest("Message timeout is not defined")
var ERR_MSG_BAD_DELIVERY_TIMEOUT *ErrorResponse = InvalidRequest("Bad delivery interval specified")

// TODO: Include allowed time out limits.
var ERR_MSG_TIMEOUT_IS_WRONG *ErrorResponse = InvalidRequest("Message timeout value is wrong")
var ERR_MSG_LIMIT_IS_WRONG *ErrorResponse = InvalidRequest("Message limit value is wrong")

var ERR_CMD_WITH_NO_PARAMS *ErrorResponse = InvalidRequest("Command doesn't accept any parameters")

var ERR_UNKNOWN_ERROR *ErrorResponse = NewError("Unknown server error", 500)
