package common

import (
	"bytes"
	"github.com/op/go-logging"
	"strconv"
)

// All responses returned to the client must follow this interface.
type IResponse interface {
	GetResponse() string
}

// Error translator
var log = logging.MustGetLogger("firempq")

func TranslateError(err error) IResponse {
	if err == nil {
		return NewStrResponse("OK")
	}
	if resp, ok := err.(IResponse); ok {
		return resp
	} else {
		log.Error(err.Error())
		return NewErrorResponse()
	}
}

// Error response.
type ErrorResponse struct{}

func NewErrorResponse() *ErrorResponse {
	return &ErrorResponse{}
}

func (er *ErrorResponse) GetResponse() string {
	return "-err:500:Server error"
}

// Simple string response used to return some quick responses for commands like ping, etc.
type StrResponse struct {
	str string
}

func (r *StrResponse) GetResponse() string {
	return "+" + r.str
}

func NewStrResponse(str string) *StrResponse {
	return &StrResponse{str: str}
}

// Int simple response.
type IntResponse struct {
	val int64
}

func NewIntResponse(val int64) *IntResponse {
	return &IntResponse{val: val}
}

func (r *IntResponse) GetResponse() string {
	return ":" + strconv.FormatInt(r.val, 10)
}

// Str Array simple response.
type StrArrayResponse struct {
	val []string
}

func NewStrArrayResponse(val []string) *StrArrayResponse {
	return &StrArrayResponse{val: val}
}

func (r *StrArrayResponse) GetResponse() string {
	var buffer bytes.Buffer
	buffer.WriteString("*")
	buffer.WriteString(strconv.Itoa(len(r.val)))
	for _, v := range r.val {
		buffer.WriteString("\n")
		buffer.WriteString(v)
	}
	return buffer.String()
}

// Predefined commonly used responses.
var RESP_OK IResponse = NewStrResponse("OK")
var RESP_PONG IResponse = NewStrResponse("PONG")

// Test interface.
var _ IResponse = NewStrResponse("test")
var _ IResponse = NewIntResponse(10)
