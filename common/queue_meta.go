package common

import "strconv"

const (
	STYPE_PRIORITY_QUEUE      = "pqueue"    // Highest priority goes first.
	STYPE_DOUBLE_SIDED_QUEUE  = "dsqueue"   // Double sided queue
	STYPE_COUNTERS            = "counters"  // Atomic counters.
	STYPE_FAIR_PRIORITY_QUEUE = "fairqueue" // POPs are fairly distributed across all priorities.
)

func NewServiceDescription(sType string, exportId uint64, name string) *ServiceDescription {
	return &ServiceDescription{
		ExportId: exportId,
		SType:    sType,
		Name:     name,
		CreateTs: Uts(),
		Disabled: false,
	}
}

func NewServiceDescriptionFromBinary(data []byte) (*ServiceDescription, error) {
	sd := ServiceDescription{}
	err := sd.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &sd, nil
}

type ServiceDescriptionList []*ServiceDescription

func (p ServiceDescriptionList) Len() int           { return len(p) }
func (p ServiceDescriptionList) Less(i, j int) bool { return p[i].ExportId < p[j].ExportId }
func (p ServiceDescriptionList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// MakeServiceId creates a string value of service INT id.
func MakeServiceId(desc *ServiceDescription) string {
	return strconv.FormatUint(desc.ExportId, 36)
}
