package iface

import (
	"firempq/defs"
)

type IItemMetaData interface {
	GetId() string
	Marshal() (data []byte, err error)
}

type IItem interface {
	GetId() string
	GetPayload() string
}

// All responses returned to the client must follow this interface.
type IResponse interface {
	GetResponse() string
	IsError() bool
}

type ISvc interface {
	IsClosed() bool
	StartUpdate()
	GetStatus() map[string]interface{}
	GetType() defs.ServiceType
	GetTypeName() string
	Call(string, []string) IResponse
	Clear()
	Close()
}
