package resp

import (
	"bytes"

	"bufio"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/enc"
)

type MessagesResponse struct {
	items []apis.IResponseItem
}

func NewItemsResponse(items []apis.IResponseItem) *MessagesResponse {
	return &MessagesResponse{
		items: items,
	}
}

func (r *MessagesResponse) GetItems() []apis.IResponseItem {
	return r.items
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
	err = enc.WriteArraySize(buf, len(r.items))
	for _, item := range r.items {
		err = buf.WriteByte(' ')
		err = item.WriteResponse(buf)
	}
	return err
}

func (r *MessagesResponse) IsError() bool {
	return false
}
