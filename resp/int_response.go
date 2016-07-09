package resp

import (
	"bytes"

	"github.com/vburenin/firempq/enc"
)

// IntResponse is a simple integer response.
type IntResponse struct {
	Value int64
}

func NewIntResponse(val int64) *IntResponse {
	return &IntResponse{Value: val}
}

func (r *IntResponse) GetStringResponse() string {
	var buf bytes.Buffer
	r.WriteResponse(&buf)
	return buf.String()
}

func (r *IntResponse) WriteResponse(buf *bytes.Buffer) error {
	_, err := buf.WriteString("+DATA ")
	enc.WriteInt64(buf, r.Value)
	return err
}

func (r *IntResponse) IsError() bool {
	return false
}
