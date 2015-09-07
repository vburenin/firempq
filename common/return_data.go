package common

import (
	"fmt"
	"strconv"
	"strings"
)

type CallFuncType func([]string) IResponse

type ItemsResponse struct {
	items []IItem
}

func NewItemsResponse(items []IItem) *ItemsResponse {
	return &ItemsResponse{items}
}

func (self *ItemsResponse) GetResponse() string {
	data := make([]string, 0, 3+9*len(self.items))
	data = append(data, "+DATA %")
	data = append(data, strconv.Itoa(len(self.items)))
	for _, item := range self.items {
		data = append(data, " ")
		itemId := item.GetId()
		itemData := item.GetContent()
		data = append(data, "$")
		data = append(data, strconv.Itoa(len(itemId)))
		data = append(data, " ")
		data = append(data, itemId)
		data = append(data, "$")
		data = append(data, strconv.Itoa(len(itemData)))
		data = append(data, " ")
		data = append(data, itemData)
	}
	return strings.Join(data, "")
}

func (self *ItemsResponse) IsError() bool {
	return false
}

// Error response.
type ErrorResponse struct {
	ErrorText string
	ErrorCode int64
}

func (e *ErrorResponse) Error() string {
	return e.ErrorText
}

func (e *ErrorResponse) GetResponse() string {
	return fmt.Sprintf("-ERR:%d:%s", e.ErrorCode, e.ErrorText)
}

func (e *ErrorResponse) IsError() bool {
	return true
}

// Error response.
type OkResponse struct {
	Code int64
}

func (e *OkResponse) GetResponse() string {
	return fmt.Sprintf("+OK:%d", e.Code)
}

func (self *OkResponse) IsError() bool {
	return false
}

var OK200_RESPONSE *OkResponse = &OkResponse{200}
