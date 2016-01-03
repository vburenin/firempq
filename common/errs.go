package common

import "fmt"

var CODE_INVALID_REQ int64 = 400
var CODE_NOT_FOUND int64 = 404
var CODE_CONFLICT_REQ int64 = 409
var CODE_SERVER_ERR int64 = 500
var CODE_SERVER_UNAVAILABLE int64 = 501
var CODE_GONE int64 = 410
var CODE_TEMPORARY_ERROR int64 = 412

func NewError(errorText string, errorCode int64) *ErrorResponse {
	return &ErrorResponse{errorText, errorCode}
}

func InvalidRequest(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CODE_INVALID_REQ}
}

func UnknownParam(paramName string) *ErrorResponse {
	return InvalidRequest(fmt.Sprintf("Unknown parameter: %s", paramName))
}

func NotFoundRequest(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CODE_NOT_FOUND}
}

func ConflictRequest(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CODE_CONFLICT_REQ}
}

func TemporaryError(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CODE_TEMPORARY_ERROR}
}

func ServerError(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CODE_SERVER_ERR}
}

var ERR_UNKNOWN_CMD = InvalidRequest("Unknown CMD")

var ERR_NO_SVC = InvalidRequest("Service is not created")
var ERR_SVC_UNKNOWN_TYPE = InvalidRequest("Unknown service type")
var ERR_SVC_ALREADY_EXISTS = ConflictRequest("Service exists already")
var ERR_ITEM_ALREADY_EXISTS = ConflictRequest("Message exists already")
var ERR_MSG_NOT_LOCKED = InvalidRequest("Message is not locked")
var ERR_MSG_NOT_FOUND = NotFoundRequest("Message not found")
var ERR_MSG_IS_LOCKED = ConflictRequest("Message is locked")
var ERR_PRIORITY_OUT_OF_RANGE = InvalidRequest("The priority is out of range")

var ERR_CONN_CLOSING = NewError("Connection will be closed soon", CODE_SERVER_UNAVAILABLE)

// Parameter errors.
var ERR_MSG_ID_NOT_DEFINED = InvalidRequest("Message ID is not defined")
var ERR_MSG_TIMEOUT_NOT_DEFINED = InvalidRequest("Message timeout is not defined")
var ERR_ASYNC_WAIT = InvalidRequest("ASYNC param can be used only if WAIT timeout greater than 0")
var ERR_ASYNC_PUSH = InvalidRequest("ASYNC must be used with SYNCWAIT")
var ERR_ID_IS_WRONG = InvalidRequest("Only [_a-zA-Z0-9]* symbols are allowed for id")
var ERR_USER_ID_IS_WRONG = InvalidRequest("Only ^[a-zA-Z0-9][_a-zA-Z0-9]* symbols are allowed for id")

var ERR_CMD_WITH_NO_PARAMS = InvalidRequest("Command doesn't accept any parameters")
var ERR_CMD_PARAM_NOT_PROVIDED = InvalidRequest("At least one parameter should be provided")
var ERR_UNKNOWN_ERROR = NewError("Unknown server error", 500)

var ERR_TS_PARAMETER_NEEDED = InvalidRequest("TS parameters must be provided")

var ERR_SIZE_EXCEEDED = TemporaryError("Service capacity reached its limit")
