package common

import "strconv"

const (
	STYPE_PRIORITY_QUEUE = "pqueue" // High priority goes first.
)

func NewServiceDescription(name, sType string, exportId uint64) *ServiceDescription {
	return &ServiceDescription{
		ExportId:  exportId,
		SType:     sType,
		Name:      name,
		CreateTs:  Uts(),
		Disabled:  false,
		ToDelete:  false,
		ServiceId: MakeServiceId(exportId),
	}
}

func UnmarshalServiceDesc(data []byte) (*ServiceDescription, error) {
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
func MakeServiceId(exportId uint64) string {
	return strconv.FormatUint(exportId, 36)
}
