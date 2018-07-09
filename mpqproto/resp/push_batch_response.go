package resp

import (
	"bufio"
	"bytes"
	"strconv"

	"github.com/vburenin/firempq/apis"
)

// IntResponse is a simple integer response.
type BatchResponse struct {
	elements []apis.IResponse
}

func NewBatchResponse(elements []apis.IResponse) *BatchResponse {
	return &BatchResponse{elements: elements}
}

func (r *BatchResponse) StringResponse() string {
	var buf bytes.Buffer
	wb := bufio.NewWriter(&buf)
	r.WriteResponse(wb)
	wb.Flush()
	return buf.String()
}

func (r *BatchResponse) WriteResponse(buf *bufio.Writer) error {
	buf.WriteString("+BATCH ")
	buf.WriteByte('*')
	s := len(r.elements) - 1
	_, err := buf.WriteString(strconv.Itoa(len(r.elements)))
	buf.WriteByte('\n')
	for i, e := range r.elements {
		err = e.WriteResponse(buf)
		if i < s {
			buf.WriteByte('\n')
		}
	}
	return err
}

func (r *BatchResponse) IsError() bool {
	return false
}
