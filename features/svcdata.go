package features

import (
	"firempq/common"
	"firempq/db"
	"firempq/log"

	. "firempq/api"
)

const ServiceConfigPrefix = ":config:"
const ServiceDescPrefix = ":desc:"

func cfgKey(serviceId string) string {
	return ServiceConfigPrefix + serviceId
}

func descKey(serviceId string) string {
	return ServiceDescPrefix + serviceId
}

func LoadServiceConfig(serviceId string, cfg Marshalable) error {
	db := db.GetDatabase()
	data := db.GetData(cfgKey(serviceId))
	if data == "" {
		return common.NotFoundRequest("No service settings found: " + serviceId)
	}

	if err := cfg.Unmarshal([]byte(data)); err != nil {
		log.Error("Error in '%s' service settings: %s", serviceId, err.Error())
		return common.ServerError("Service settings error: " + serviceId)
	}
	return nil
}

// SaveServiceConfig saves service config into database.
func SaveServiceConfig(serviceId string, conf MarshalToBin) error {
	db := db.GetDatabase()
	data, _ := conf.Marshal()
	err := db.StoreData(cfgKey(serviceId), string(data))
	if err != nil {
		log.Error("Failed to save config: %s", err.Error())
		return common.ServerError("Can not save service data: " + serviceId)
	}
	return nil
}

// GetServiceDescriptions Loads all service descriptions prefixed with ServiceDescPrefix
func GetServiceDescriptions() common.ServiceDescriptionList {
	sdList := make(common.ServiceDescriptionList, 0, 16)
	db := db.GetDatabase()
	descIter := db.IterData(ServiceDescPrefix)
	defer descIter.Close()

	for ; descIter.Valid(); descIter.Next() {
		svcDesc, err := common.UnmarshalServiceDesc([]byte(descIter.GetValue()))
		if err != nil {
			log.Error("Coudn't read service '%s' description: %s", descIter.GetTrimKey(), err.Error())
			continue
		}
		sdList = append(sdList, svcDesc)
	}
	return sdList
}

// GetServiceDescriptions Loads all service descriptions prefixed with ServiceDescPrefix
func GetServiceDescription(serviceId string) *common.ServiceDescription {
	db := db.GetDatabase()
	data := db.GetData(descKey(serviceId))
	desc, _ := common.UnmarshalServiceDesc([]byte(data))
	return desc
}

// SaveServiceDescription saves service config into database.
func SaveServiceDescription(desc *common.ServiceDescription) error {
	db := db.GetDatabase()
	data, _ := desc.Marshal()
	return db.StoreData(desc.ServiceId, string(data))
}

func DeleteServiceData(serviceId string) {
	desc := GetServiceDescription(serviceId)
	if desc == nil {
		log.Error("Attempt to delete unknown service id: %s", serviceId)
	}
	desc.ToDelete = true
	SaveServiceDescription(desc)

	db := db.GetDatabase()
	db.DeleteDataWithPrefix(serviceId)
	db.DeleteData(cfgKey(serviceId))
	db.DeleteData(descKey(serviceId))
}
