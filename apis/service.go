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
