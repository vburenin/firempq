package resp

import (
	"bufio"
	"bytes"

	"github.com/vburenin/firempq/enc"
)

// StrResponse is a simple string response used to return some quick responses for commands like ping, etc.
type MsgResponse struct {
	MsgId string
}

func NewMsgResponse(msgId string) *MsgResponse {
	return &MsgResponse{
		MsgId: msgId,
	}
}

func (r *MsgResponse) StringResponse() string {
	var buf bytes.Buffer
	wb := bufio.NewWriter(&buf)
	r.WriteResponse(wb)
	wb.Flush()
	return buf.String()
}

func (r *MsgResponse) WriteResponse(buf *bufio.Writer) error {
	buf.WriteString("+MSG ")
	return enc.WriteString(buf, r.MsgId)
}

func (r *MsgResponse) IsError() bool {
	return false
}
