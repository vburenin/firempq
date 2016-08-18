package resp

import (
	"bufio"
	"bytes"

	"github.com/vburenin/firempq/enc"
)

type DictResponse struct {
	dict   map[string]interface{}
	header string
}

func NewDictResponse(header string, dict map[string]interface{}) *DictResponse {
	return &DictResponse{
		dict:   dict,
		header: header,
	}
}

func (r *DictResponse) GetDict() map[string]interface{} {
	return r.dict
}

func (r *DictResponse) StringResponse() string {
	var buf bytes.Buffer
	wb := bufio.NewWriter(&buf)
	r.WriteResponse(wb)
	wb.Flush()
	return buf.String()
}

func (r *DictResponse) WriteResponse(buf *bufio.Writer) error {
	var err error
	if len(r.header) > 0 {
		_, err = buf.WriteString(r.header)
		err = buf.WriteByte(' ')
	}

	enc.WriteDict(buf, r.dict)
	return err
}

func (r *DictResponse) IsError() bool { return false }
