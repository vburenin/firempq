package queue_info

import (
	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/db"
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/utils"
)

const ServiceConfigPrefix = ":config:"
const ServiceDescPrefix = ":desc:"

func cfgKey(serviceId string) string {
	return ServiceConfigPrefix + serviceId
}

func descKey(serviceId string) string {
	return ServiceDescPrefix + serviceId
}

func LoadServiceConfig(serviceId string, cfg apis.Marshalable) error {
	db := db.DatabaseInstance()
	data := db.GetData(cfgKey(serviceId))
	if len(data) == 0 {
		return mpqerr.NotFoundRequest("No service settings found: " + serviceId)
	}

	if err := cfg.Unmarshal(data); err != nil {
		log.Error("Error in '%s' service settings: %s", serviceId, err.Error())
		return mpqerr.ServerError("Service settings error: " + serviceId)
	}
	return nil
}

// SaveServiceConfig saves service config into database.
func SaveServiceConfig(serviceId string, conf apis.MarshalToBin) error {
	db := db.DatabaseInstance()
	data, _ := conf.Marshal()
	err := db.StoreData(cfgKey(serviceId), data)
	if err != nil {
		log.Error("Failed to save config: %s", err.Error())
		return mpqerr.ServerError("Can not save service data: " + serviceId)
	}
	return nil
}

// GetServiceDescriptions Loads all service descriptions prefixed with ServiceDescPrefix
func GetServiceDescriptions() ServiceDescriptionList {
	sdList := make(ServiceDescriptionList, 0, 16)
	db := db.DatabaseInstance()
	descIter := db.IterData(ServiceDescPrefix)
	defer descIter.Close()

	for ; descIter.Valid(); descIter.Next() {
		svcDesc, err := UnmarshalServiceDesc([]byte(descIter.GetValue()))
		if err != nil {
			log.Error("Coudn't read service '%s' description: %s", descIter.GetTrimKey(), err.Error())
			continue
		}
		sdList = append(sdList, svcDesc)
	}
	return sdList
}

// GetServiceDescriptions Loads all service descriptions prefixed with ServiceDescPrefix
func GetServiceDescription(serviceId string) *ServiceDescription {
	db := db.DatabaseInstance()
	data := db.GetData(descKey(serviceId))
	desc, _ := UnmarshalServiceDesc([]byte(data))
	return desc
}

// SaveServiceDescription saves service config into database.
func SaveServiceDescription(desc *ServiceDescription) error {
	db := db.DatabaseInstance()
	data, _ := desc.Marshal()
	return db.StoreData(descKey(desc.ServiceId), data)
}

func DeleteServiceData(serviceId string) {
	desc := GetServiceDescription(serviceId)
	if desc == nil {
		log.Error("Attempt to delete unknown service id: %s", serviceId)
		return
	}
	desc.ToDelete = true
	SaveServiceDescription(desc)

	db := db.DatabaseInstance()
	db.DeleteDataWithPrefix(serviceId)
	db.DeleteData(cfgKey(serviceId))
	db.DeleteData(descKey(serviceId))
}

func NewServiceDescription(name, sType string, exportId uint64) *ServiceDescription {
	return &ServiceDescription{
		ExportId:  exportId,
		SType:     sType,
		Name:      name,
		CreateTs:  utils.Uts(),
		Disabled:  false,
		ToDelete:  false,
		ServiceId: enc.EncodeTo36Base(exportId),
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
