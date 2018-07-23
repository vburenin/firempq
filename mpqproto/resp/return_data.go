package resp

import (
	"bufio"
	"bytes"

	"github.com/vburenin/firempq/export/encoding"
	"github.com/vburenin/firempq/pmsg"
)

type MessagesResponse struct {
	Messages []*pmsg.FullMessage
}

func NewItemsResponse(items []*pmsg.FullMessage) *MessagesResponse {
	return &MessagesResponse{
		Messages: items,
	}
}

func (r *MessagesResponse) StringResponse() string {
	var buf bytes.Buffer
	wb := bufio.NewWriter(&buf)
	r.WriteResponse(wb)
	wb.Flush()
	return buf.String()
}

func (r *MessagesResponse) WriteResponse(buf *bufio.Writer) error {
	_, err := buf.WriteString("+MSGS ")
	err = encoding.WriteArraySize(buf, len(r.Messages))
	for _, item := range r.Messages {
		err = buf.WriteByte(' ')
		err = item.WriteResponse(buf)
	}
	return err
}

func (r *MessagesResponse) IsError() bool {
	return false
}
