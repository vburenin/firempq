package resp

import (
	"bufio"
	"bytes"
)

// StrResponse is a simple string response used to return some quick responses for commands like ping, etc.
type StrResponse struct {
	prefix string
	data   string
}

func NewStrResponse(data string) *StrResponse {
	return &StrResponse{
		prefix: "+",
		data:   data,
	}
}

func NewAsyncAccept(data string) *StrResponse {
	return &StrResponse{
		prefix: "+A ",
		data:   data,
	}
}

func (r *StrResponse) GetStringResponse() string {
	var buf bytes.Buffer
	wb := bufio.NewWriter(&buf)
	r.WriteResponse(wb)
	wb.Flush()
	return buf.String()
}

func (r *StrResponse) WriteResponse(buf *bufio.Writer) error {
	_, err := buf.WriteString(r.prefix)
	_, err = buf.WriteString(r.data)
	return err
}

func (r *StrResponse) IsError() bool {
	return false
}
