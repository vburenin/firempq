package common

import "firempq/defs"

type Marshalable interface {
	Marshal() (data []byte, err error)
	Unmarshal(data []byte) error
}

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
	// TimedCalls is called periodically where ts passed as a parameter.
	// Returns True if action is there is more work to do.
	Update(ts int64) bool
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
