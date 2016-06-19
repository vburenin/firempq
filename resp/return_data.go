package resp

import (
	"io"
	"strconv"
	"strings"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/enc"
)

type CallFuncType func([]string) apis.IResponse

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

func (self *DictResponse) GetDict() map[string]interface{} {
	return self.dict
}

func (self *DictResponse) getResponseChunks() []string {
	data := make([]string, 0, 3+9*len(self.dict))
	data = append(data, self.header)
	data = append(data, " %")
	data = append(data, strconv.Itoa(len(self.dict)))
	for k, v := range self.dict {
		data = append(data, " ")
		data = append(data, k)
		switch t := v.(type) {
		case string:
			data = append(data, enc.EncodeString(t))
		case int:
			data = append(data, enc.EncodeInt64(int64(t)))
		case int64:
			data = append(data, enc.EncodeInt64(t))
		case bool:
			data = append(data, enc.EncodeBool(t))
		}
	}
	return data
}

func (self *DictResponse) GetResponse() string {
	return strings.Join(self.getResponseChunks(), "")
}

func (self *DictResponse) WriteResponse(buff io.Writer) error {
	_, err := buff.Write(enc.UnsafeStringToBytes(self.GetResponse()))
	return err
}

func (self *DictResponse) IsError() bool { return false }

type ItemsResponse struct {
	items []apis.IResponseItem
}

func NewItemsResponse(items []apis.IResponseItem) *ItemsResponse {
	return &ItemsResponse{
		items: items,
	}
}

func (self *ItemsResponse) GetItems() []apis.IResponseItem {
	return self.items
}

func (self *ItemsResponse) getResponseChunks() []string {
	data := make([]string, 0, 3+9*len(self.items))
	data = append(data, "+MSGS")
	data = append(data, enc.EncodeArraySize(len(self.items)))
	for _, item := range self.items {
		data = append(data, item.Encode())
	}
	return data
}

func (self *ItemsResponse) GetResponse() string {
	return strings.Join(self.getResponseChunks(), "")
}

func (self *ItemsResponse) WriteResponse(buff io.Writer) error {
	_, err := buff.Write(enc.UnsafeStringToBytes(self.GetResponse()))
	return err
}

func (self *ItemsResponse) IsError() bool {
	return false
}
