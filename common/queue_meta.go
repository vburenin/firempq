package common

import (
	"firempq/util"
)

const (
	QTYPE_PRIORITY_QUEUE      = "priority_queue" // Highest priority goes first.
	QTYPE_FIFO_QUEUE          = "fifo_queue"     // Standard FIFO
	QTYPE_DOUBLE_SIDED_QUEUE  = "ds_queue"     	 // Double sided queue
	QTYPE_FAIR_PRIORITY_QUEUE = "fair_queue"     // POPs are fairly distributed across all priorities.
	QTYPE_COUNTERS            = "counters"       // Atomic counters.
	QTYPE_SEQUENCE_READ       = "seqreader"      // Data to read in sequential order.
)

type QueueMetaInfo struct {
	Qtype    string
	Id       int32
	Name     string
	CreateTs int64
	Disabled bool
}

func NewQueueMetaInfo(qtype string, id int32, name string) *QueueMetaInfo {
	return &QueueMetaInfo{
		Qtype:    qtype,
		Id:       id,
		Name:     name,
		CreateTs: util.Uts(),
		Disabled: false,
	}
}

func QueueInfoFromBinary(data []byte) (*QueueMetaInfo, error) {
	qmi := QueueMetaInfo{}
	err := util.StructFromBinary(&qmi, data)
	if err != nil {
		return nil, err
	}
	return &qmi, nil
}

func (q *QueueMetaInfo) ToBinary() []byte {
	return util.StructToBinary(q)
}
