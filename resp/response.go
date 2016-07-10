package resp

import (
	"bufio"
	"bytes"

	"github.com/vburenin/firempq/apis"
)

type AsyncResponse struct {
	asyncID string
	resp    apis.IResponse
}

func NewAsyncResponse(asyncID string, resp apis.IResponse) apis.IResponse {
	return &AsyncResponse{
		asyncID: asyncID,
		resp:    resp,
	}
}

func (r *AsyncResponse) GetStringResponse() string {
	var buf bytes.Buffer
	wb := bufio.NewWriter(&buf)
	r.WriteResponse(wb)
	wb.Flush()
	return buf.String()
}

func (r *AsyncResponse) IsError() bool {
	return r.resp.IsError()
}

func (r *AsyncResponse) WriteResponse(buf *bufio.Writer) error {
	_, err := buf.WriteString("+ASYNC ")
	_, err = buf.WriteString(r.asyncID)
	err = buf.WriteByte(' ')
	err = r.resp.WriteResponse(buf)
	return err
}

var RESP_PONG = NewStrResponse("PONG")
var OK_RESPONSE = NewStrResponse("OK")
