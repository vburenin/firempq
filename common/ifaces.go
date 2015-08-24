package common

import "firempq/defs"

type IItemMetaData interface {
	GetId() string
	ToBinary() []byte
	GetStatus() map[string]interface{}
}

type IItem interface {
	GetId() string
	GetContent() string
	GetContentType() defs.DataType
	GetStatus() map[string]interface{}
}

type ISvc interface {
	GetStatus() map[string]interface{}
	GetType() defs.ServiceType
	GetTypeName() string
	Call(action string, params map[string]string) *ReturnData
	Clear()
	Close()
}

type IServer interface {
	Start()
	Stop()
}

type CallFuncType func(map[string]string) *ReturnData

type ReturnData struct {
	Items []IItem       // Optional array of returned items.
	Code  defs.RespCode // response code if needed.
	Msg   string        // Text message that may be returned to the caller.
	Err   error         // Optional error.
}

func NewRetDataError(err error) *ReturnData {
	return &ReturnData{Err: err}
}

func NewRetDataMessage(msg string, code defs.RespCode) *ReturnData {
	return &ReturnData{Msg: msg, Code: code}
}

func NewRetData(msg string, code defs.RespCode, items []IItem) *ReturnData {
	return &ReturnData{Msg: msg, Code: code, Items: items}
}

var RETDATA_200OK *ReturnData = &ReturnData{Code: defs.CODE_200_OK, Msg: "OK"}
var RETDATA_201OK *ReturnData = &ReturnData{Code: defs.CODE_201_OK, Msg: "OK"}
