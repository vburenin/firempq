package api

type MarshalToBin interface {
	Marshal() (data []byte, err error)
}

type UnmarshalFromBin interface {
	Unmarshal(data []byte) error
}

type Marshalable interface {
	MarshalToBin
	UnmarshalFromBin
}
