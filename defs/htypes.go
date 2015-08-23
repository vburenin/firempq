package defs

type ServiceType int

const (
	HT_PRIORITY_QUEUE     ServiceType = 1
	HT_COUNTERS           ServiceType = 2
	HT_DOUBLE_SIDED_QUEUE ServiceType = 3
)

type DataType int

const (
	DT_STR       DataType = 1
	DT_INT       DataType = 2
	DT_LIST      DataType = 3
	DT_FLOAT     DataType = 4
	DT_JSON      DataType = 5
	DT_INT_SET   DataType = 6
	DT_FLOAT_SET DataType = 7
	DT_STR_SET   DataType = 8
)
