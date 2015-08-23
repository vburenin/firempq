package common

import "strconv"

// All responses returned to the client must follow this interface.
type IResponse interface {
	GetResponse() string
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

// Test interface.
var _ IResponse = NewStrResponse("test")
var _ IResponse = NewIntResponse(10)
