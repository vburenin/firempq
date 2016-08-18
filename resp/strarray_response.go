package resp

import (
	"bufio"
	"bytes"
	"strconv"

	"github.com/vburenin/firempq/enc"
)

// StrArrayResponse a response containing array of strings.
type StrArrayResponse struct {
	val    []string
	header string
}

func NewStrArrayResponse(header string, val []string) *StrArrayResponse {
	return &StrArrayResponse{
		val:    val,
		header: header,
	}
}

func (r *StrArrayResponse) IsError() bool {
	return false
}

func (r *StrArrayResponse) StringResponse() string {
	var buf bytes.Buffer
	wb := bufio.NewWriter(&buf)
	r.WriteResponse(wb)
	wb.Flush()
	return buf.String()
}

func (r *StrArrayResponse) WriteResponse(buf *bufio.Writer) error {
	_, err := buf.WriteString(r.header)
	_, err = buf.WriteString(" *")
	_, err = buf.WriteString(strconv.Itoa(len(r.val)))
	for _, v := range r.val {
		err = buf.WriteByte(' ')
		enc.WriteString(buf, v)
	}
	return err
}
