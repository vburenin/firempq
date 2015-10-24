package common

import (
	"bytes"
	"firempq/iface"
	"strconv"

	"github.com/op/go-logging"
)

// Error translator
var log = logging.MustGetLogger("firempq")

func TranslateError(err error) iface.IResponse {
	if err == nil {
		return OK_RESPONSE
	}
	if resp, ok := err.(iface.IResponse); ok {
		return resp
	} else {
		log.Error(err.Error())
		return ERR_UNKNOWN_ERROR
	}
}

// Simple string response used to return some quick responses for commands like ping, etc.
type StrResponse struct {
	str string
}

func NewStrResponse(str string) *StrResponse {
	return &StrResponse{str: str}
}

func (r *StrResponse) GetResponse() string {
	return "+" + r.str
}

func (r *StrResponse) IsError() bool {
	return false
}

// Int simple response.
type IntResponse struct {
	Value int64
}

func NewIntResponse(val int64) *IntResponse {
	return &IntResponse{Value: val}
}

func (r *IntResponse) GetResponse() string {
	return "+DATA :" + strconv.FormatInt(r.Value, 10)
}

func (r *IntResponse) IsError() bool {
	return false
}

// Str Array simple response.
type StrArrayResponse struct {
	val []string
}

func NewStrArrayResponse(val []string) *StrArrayResponse {
	return &StrArrayResponse{val: val}
}

func (r *StrArrayResponse) IsError() bool {
	return false
}

func (r *StrArrayResponse) GetResponse() string {
	var buffer bytes.Buffer
	buffer.WriteString("+DATA *")
	buffer.WriteString(strconv.Itoa(len(r.val)))
	for _, v := range r.val {
		buffer.WriteString("\n")
		buffer.WriteString(v)
	}
	return buffer.String()
}

// Predefined commonly used responses.
var RESP_PONG iface.IResponse = NewStrResponse("PONG")

// Test interface.
var _ iface.IResponse = NewStrResponse("test")
var _ iface.IResponse = NewIntResponse(10)
