package api

import (
	"io"
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
	// WriteResponse writes the response data directly into writer.
	WriteResponse(io.Writer) error
	// IsError tells if this response is actually an error.
	IsError() bool
}

type ISvc interface {
	NewContext() ServiceContext
	StartUpdate()
	GetTypeName() string
	GetSize() int
	GetStatus() map[string]interface{}
	GetServiceId() string
	Clear()
	Close()
	IsClosed() bool
}

type ServiceContext interface {
	Call(cmd string, params []string) IResponse
	Finish()
}
