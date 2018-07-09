package mpqerr

import (
	"bufio"
	"bytes"
	"fmt"

	"github.com/vburenin/firempq/enc"
)

var CodeInvalidReq int64 = 400
var CodeNotFound int64 = 404
var CodeConflictReq int64 = 409
var CodeServerError int64 = 500
var CodeServerUnavailable int64 = 501
var CodeTemporaryError int64 = 412

// ErrorResponse is an error response.
type ErrorResponse struct {
	Text string
	Code int64 // code length must always be 3 digits.
}

func (e *ErrorResponse) Error() string {
	return e.Text
}

func (e *ErrorResponse) StringResponse() string {
	var buf bytes.Buffer
	wb := bufio.NewWriter(&buf)
	e.WriteResponse(wb)
	wb.Flush()
	return buf.String()
}

func (e *ErrorResponse) WriteResponse(buf *bufio.Writer) error {
	return enc.WriteError(buf, e.Code, e.Text)
}

func (e *ErrorResponse) IsError() bool {
	return true
}

func NewError(errorText string, errorCode int64) *ErrorResponse {
	return &ErrorResponse{errorText, errorCode}
}

func InvalidRequest(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CodeInvalidReq}
}

func UnknownParam(paramName string) *ErrorResponse {
	return InvalidRequest(fmt.Sprintf("Unknown parameter: %s", paramName))
}

func NotFoundRequest(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CodeNotFound}
}

func ConflictRequest(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CodeConflictReq}
}

func TemporaryError(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CodeTemporaryError}
}

func ServerError(errorText string) *ErrorResponse {
	return &ErrorResponse{errorText, CodeServerError}
}

var ErrNoQueue = InvalidRequest("queue is not created")
var ErrInvalidQueueName = InvalidRequest("name can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length")
var ErrQueueAlreadyExists = ConflictRequest("queue exists already")
var ErrMsgAlreadyExists = ConflictRequest("message exists already")
var ErrMsgNotLocked = InvalidRequest("message is not locked")
var ErrMsgNotFound = NotFoundRequest("message not found")
var ErrMsgLocked = ConflictRequest("message is locked")

var ErrInvalidRcpt = InvalidRequest("receipt is invalid")
var ErrNoRcpt = InvalidRequest("no receipt provided")
var ErrExpiredRcpt = InvalidRequest("Receipt has expired")
var ErrOneRcptOnly = InvalidRequest("Only one receipt at the time is currently supported")

var ErrConnClosing = NewError("Connection will be closed soon", CodeServerUnavailable)

// Parameter errors.
var ErrMsgIdNotDefined = InvalidRequest("Message ID is not defined")
var ErrMsgTimeoutNotDefined = InvalidRequest("Message timeout is not defined")
var ErrAsyncWait = InvalidRequest("ASYNC param can be used only if WAIT timeout greater than 0")
var ErrAsyncPush = InvalidRequest("ASYNC must be used with SYNCWAIT")
var ErrInvalidID = InvalidRequest(
	"ID length must be in range[1, 256]. Only [_a-zA-Z0-9]* symbols are allowed for id")
var ErrInvalidUserID = InvalidRequest(
	"ID length must be in range[1, 256]. Only ^[a-zA-Z0-9][_a-zA-Z0-9]* symbols are allowed for id")

var ErrOneIdOnly = InvalidRequest("only one ID at the time is currently supported")

var ErrCmdNoParamsAllowed = InvalidRequest("command doesn't accept any parameters")
var ErrCmdNoParams = InvalidRequest("at least one parameter should be provided")

var ErrNoTsParam = InvalidRequest("TS parameters must be provided")

var ErrSizeExceeded = TemporaryError("queue capacity reached its limit")
var ErrDbProblem = TemporaryError("database returned error")

// Parsers errors

var ErrTokTooManyTokens = InvalidRequest("too many tokens")
var ErrTokTooLong = InvalidRequest("token is too long")
var ErrTokInvalid = InvalidRequest("error during token parsing")
