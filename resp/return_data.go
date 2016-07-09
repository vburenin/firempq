package resp

import (
	"bytes"

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

func (r *MessagesResponse) GetStringResponse() string {
	var buf bytes.Buffer
	r.WriteResponse(&buf)
	return buf.String()
}

func (r *MessagesResponse) WriteResponse(buf *bytes.Buffer) error {
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
