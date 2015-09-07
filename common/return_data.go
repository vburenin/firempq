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

type DictResponse struct {
	dict map[string]interface{}
}

func NewDictResponse(dict map[string]interface{}) *DictResponse {
	return &DictResponse{dict}
}

func (self *DictResponse) GetResponse() string {
	data := make([]string, 0, 3+9*len(self.dict))
	data = append(data, "+DATA %")
	data = append(data, strconv.Itoa(len(self.dict)))
	for k, v := range self.dict {
		data = append(data, "\n")
		data = append(data, k)
		data = append(data, " ")
		switch t := v.(type) {
		case string:
			data = append(data, t)
		case int:
			data = append(data, ":")
			data = append(data, strconv.Itoa(t))
		case int64:
			data = append(data, ":")
			data = append(data, strconv.Itoa(int(t)))
		}
	}
	return strings.Join(data, "")
}

func (self *DictResponse) IsError() bool {
	return false
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
