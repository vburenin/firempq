package errors

import (
	"fmt"
	"io"

	. "firempq/encoding"
	. "firempq/utils"
)

var CODE_INVALID_REQ int64 = 400
var CODE_NOT_FOUND int64 = 404
var CODE_CONFLICT_REQ int64 = 409
var CODE_SERVER_ERR int64 = 500
var CODE_SERVER_UNAVAILABLE int64 = 501
var CODE_GONE int64 = 410
var CODE_TEMPORARY_ERROR int64 = 412

// ErrorResponse is an error response.
type ErrorResponse struct {
	ErrorText string
	ErrorCode int64
}

func (e *ErrorResponse) Error() string {
	return e.ErrorText
}

func (e *ErrorResponse) GetResponse() string {
	return EncodeError(e.ErrorCode, e.ErrorText)
}

func (e *ErrorResponse) WriteResponse(buff io.Writer) error {
	_, err := buff.Write(UnsafeStringToBytes(e.GetResponse()))
	return err
}

func (e *ErrorResponse) IsError() bool {
	return true
}

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

var ERR_NO_SVC = InvalidRequest("Service is not created")
var ERR_SVC_UNKNOWN_TYPE = InvalidRequest("Unknown service type")
var ERR_SVC_ALREADY_EXISTS = ConflictRequest("Service exists already")
var ERR_ITEM_ALREADY_EXISTS = ConflictRequest("Message exists already")
var ERR_MSG_NOT_LOCKED = InvalidRequest("Message is not locked")
var ERR_MSG_NOT_FOUND = NotFoundRequest("Message not found")
var ERR_MSG_IS_LOCKED = ConflictRequest("Message is locked")

var ERR_INVALID_RECEIPT = InvalidRequest("Receipt is invalid")
var ERR_NO_RECEIPT = InvalidRequest("No receipt provided")
var ERR_RECEIPT_EXPIRED = InvalidRequest("Receipt has expired")
var ERR_ONE_RECEIPT_ONLY = InvalidRequest("Only one receipt at the time is currently supported")

var ERR_CONN_CLOSING = NewError("Connection will be closed soon", CODE_SERVER_UNAVAILABLE)

// Parameter errors.
var ERR_MSG_ID_NOT_DEFINED = InvalidRequest("Message ID is not defined")
var ERR_MSG_TIMEOUT_NOT_DEFINED = InvalidRequest("Message timeout is not defined")
var ERR_ASYNC_WAIT = InvalidRequest("ASYNC param can be used only if WAIT timeout greater than 0")
var ERR_ASYNC_PUSH = InvalidRequest("ASYNC must be used with SYNCWAIT")
var ERR_ID_IS_WRONG = InvalidRequest(
	"ID length must be in range[1, 256]. Only [_a-zA-Z0-9]* symbols are allowed for id")
var ERR_USER_ID_IS_WRONG = InvalidRequest(
	"ID length must be in range[1, 256]. Only ^[a-zA-Z0-9][_a-zA-Z0-9]* symbols are allowed for id")

var ERR_ONE_ID_ONLY = InvalidRequest("Only one ID at the time is currently supported")

var ERR_CMD_WITH_NO_PARAMS = InvalidRequest("Command doesn't accept any parameters")
var ERR_CMD_PARAM_NOT_PROVIDED = InvalidRequest("At least one parameter should be provided")
var ERR_UNKNOWN_ERROR = NewError("Unknown server error", 500)

var ERR_TS_PARAMETER_NEEDED = InvalidRequest("TS parameters must be provided")

var ERR_SIZE_EXCEEDED = TemporaryError("Service capacity reached its limit")

// Parsers errors

var ERR_TOK_TOO_MANY_TOKENS = InvalidRequest("Too many tokens")
var ERR_TOK_TOKEN_TOO_LONG = InvalidRequest("Token is too long")
var ERR_TOK_PARSING_ERROR = InvalidRequest("Error during token parsing")
