package resp

import (
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

func (r *StrArrayResponse) GetStringResponse() string {
	var buf bytes.Buffer
	r.WriteResponse(&buf)
	return buf.String()
}

func (r *StrArrayResponse) WriteResponse(buf *bytes.Buffer) error {
	_, err := buf.WriteString(r.header)
	_, err = buf.WriteString(" *")
	_, err = buf.WriteString(strconv.Itoa(len(r.val)))
	for _, v := range r.val {
		err = buf.WriteByte(' ')
		enc.WriteString(buf, v)
	}
	return err
}

