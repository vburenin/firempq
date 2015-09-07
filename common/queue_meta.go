package common

const (
	STYPE_PRIORITY_QUEUE      = "pqueue"    // Highest priority goes first.
	STYPE_DOUBLE_SIDED_QUEUE  = "dsqueue"   // Double sided queue
	STYPE_FIFO_QUEUE          = "fifoqueue" // Standard FIFO
	STYPE_FAIR_PRIORITY_QUEUE = "fairqueue" // POPs are fairly distributed across all priorities.
	STYPE_COUNTERS            = "counters"  // Atomic counters.
	STYPE_SEQUENCE_READ       = "seqreader" // Data to read in sequential order.
)

type ServiceMetaInfo struct {
	Stype    string
	Id       int32
	Name     string
	CreateTs int64
	Disabled bool
}

func NewServiceMetaInfo(stype string, id int32, name string) *ServiceMetaInfo {
	return &ServiceMetaInfo{
		Stype:    stype,
		Id:       id,
		Name:     name,
		CreateTs: Uts(),
		Disabled: false,
	}
}

func ServiceInfoFromBinary(data []byte) (*ServiceMetaInfo, error) {
	smi := ServiceMetaInfo{}
	err := StructFromBinary(&smi, data)
	if err != nil {
		return nil, err
	}
	return &smi, nil
}

func (q *ServiceMetaInfo) ToBinary() []byte {
	return StructToBinary(q)
}
