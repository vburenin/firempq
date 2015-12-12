package api

import (
	"firempq/defs"
)

type IItemMetaData interface {
	GetId() string
	StringMarshal() string
}

type IItem interface {
	GetId() string
	GetPayload() string
}

// IResponse is a standard interface to return as a response.
type IResponse interface {
	// GetResponse returns serialized string of data that can be returned to the client.
	GetResponse() string
	// IsError tells if this response is actually an error.
	IsError() bool
}

type ISvc interface {
	IsClosed() bool
	Size() int
	StartUpdate()
	GetStatus() map[string]interface{}
	GetType() defs.ServiceType
	GetTypeName() string
	GetServiceId() string
	Call(string, []string) IResponse
	Clear()
	Close()
}
