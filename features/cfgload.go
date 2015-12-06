package features

import (
	"firempq/common"
	"firempq/db"
	"firempq/iface"
	"firempq/log"
)

const ServiceConfigPrefix = ":config:"
const ServiceDescPrefix = ":desc:"

func cfgKey(serviceId string) string {
	return ServiceConfigPrefix + serviceId
}

func LoadServiceConfig(serviceId string, cfg iface.Marshalable) error {
	db := db.GetDatabase()
	data := db.GetData(cfgKey(serviceId))
	if data == nil {
		return common.NotFoundRequest("No service settings found: " + serviceId)
	}

	if err := cfg.Unmarshal(data); err != nil {
		log.Error("Error in '%s' service settings: %s", serviceId, err.Error())
		return common.ServerError("Service settings error: " + serviceId)
	}
	return nil
}

// SaveServiceConfig saves service config into database.
func SaveServiceConfig(serviceId string, conf iface.MarshalToBin) error {
	db := db.GetDatabase()
	data, _ := conf.Marshal()
	err := db.StoreData(cfgKey(serviceId), data)
	if err != nil {
		log.Error("Failed to save config: %s", err.Error())
		return common.ServerError("Can not save service data: " + serviceId)
	}
	return nil
}
