package common

import "firempq/defs"

type IItemMetaData interface {
	GetId() string
	ToBinary() []byte
}

type IItem interface {
	GetId() string
	GetContent() string
}

// All responses returned to the client must follow this interface.
type IResponse interface {
	GetResponse() string
	IsError() bool
}

type ISvc interface {
	IsClosed() bool
	GetStatus() map[string]interface{}
	GetType() defs.ServiceType
	GetTypeName() string
	Call(string, []string) IResponse
	Clear()
	Close()
}

type IServer interface {
	Start()
	Stop()
}
