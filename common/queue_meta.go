package common

import (
	"firempq/util"
)

const (
	STYPE_PRIORITY_QUEUE      = "priority_queue" // Highest priority goes first.
	STYPE_FIFO_QUEUE          = "fifo_queue"     // Standard FIFO
	STYPE_DOUBLE_SIDED_QUEUE  = "ds_queue"       // Double sided queue
	STYPE_FAIR_PRIORITY_QUEUE = "fair_queue"     // POPs are fairly distributed across all priorities.
	STYPE_COUNTERS            = "counters"       // Atomic counters.
	STYPE_SEQUENCE_READ       = "seqreader"      // Data to read in sequential order.
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
		CreateTs: util.Uts(),
		Disabled: false,
	}
}

func ServiceInfoFromBinary(data []byte) (*ServiceMetaInfo, error) {
	smi := ServiceMetaInfo{}
	err := util.StructFromBinary(&smi, data)
	if err != nil {
		return nil, err
	}
	return &smi, nil
}

func (q *ServiceMetaInfo) ToBinary() []byte {
	return util.StructToBinary(q)
}
