package apis

import "bufio"

// IResponseItem is an existing data response.
type IResponseItem interface {
	WriteResponse(b *bufio.Writer) error
	ID() string
	Payload() []byte
}

// IResponse is a standard interface to any returned data.
type IResponse interface {
	// GetResponse returns serialized string of data that can be returned to the client.
	StringResponse() string
	// WriteResponse writes the response data directly into writer.
	WriteResponse(buf *bufio.Writer) error
	// IsError tells if this response is actually an error.
	IsError() bool
}

// ResponseWriter is a writer of IResponse object.
type ResponseWriter interface {
	WriteResponse(IResponse) error
}

// ServiceInfo provide the most basic current information about service.
type ServiceInfo struct {
	Type string
	Size int
	ID   string
}

// ISvc a service interface used to start/stop and receive basic service info.
type ISvc interface {
	NewContext(ResponseWriter) ServiceContext
	StartUpdate()
	Info() ServiceInfo
	Close()
}

// ServiceContext is a user protocol parser in scope of a single connection.
type ServiceContext interface {
	Call(cmd string, params []string) IResponse
	Finish()
}

// IServices all instantiated service container interface.
type IServices interface {
	GetService(string) (ISvc, bool)
}
