package resp

import (
	"bufio"
	"bytes"
)

// StrResponse is a simple string response used to return some quick responses for commands like ping, etc.
type StrResponse struct {
	data string
}

func NewStrResponse(data string) *StrResponse {
	return &StrResponse{
		data: data,
	}
}

func NewAsyncAccept(data string) *StrResponse {
	return &StrResponse{
		data: "+A " + data,
	}
}

func (r *StrResponse) StringResponse() string {
	var buf bytes.Buffer
	wb := bufio.NewWriter(&buf)
	r.WriteResponse(wb)
	wb.Flush()
	return buf.String()
}

func (r *StrResponse) WriteResponse(buf *bufio.Writer) error {
	_, err := buf.WriteString(r.data)
	return err
}

func (r *StrResponse) IsError() bool {
	return false
}

var PONG = NewStrResponse("+PONG")
var OK = NewStrResponse("+OK")
var DISCONNECT = NewStrResponse("-DISCONNECT")
