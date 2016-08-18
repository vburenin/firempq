package apis

// BinaryMarshaller is the marshaling interface for binary data.
type BinaryMarshaller interface {
	Unmarshal(data []byte) error
	Marshal() (data []byte, err error)
}
