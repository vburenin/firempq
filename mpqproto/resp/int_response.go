package resp

import (
	"bufio"
	"bytes"

	"github.com/vburenin/firempq/export/encoding"
)

// IntResponse is a simple integer response.
type IntResponse struct {
	Value int64
}

func NewIntResponse(val int64) *IntResponse {
	return &IntResponse{Value: val}
}

func (r *IntResponse) StringResponse() string {
	var buf bytes.Buffer
	wb := bufio.NewWriter(&buf)
	r.WriteResponse(wb)
	wb.Flush()
	return buf.String()
}

func (r *IntResponse) WriteResponse(buf *bufio.Writer) error {
	_, err := buf.WriteString("+DATA ")
	encoding.WriteInt64(buf, r.Value)
	return err
}

func (r *IntResponse) IsError() bool {
	return false
}
