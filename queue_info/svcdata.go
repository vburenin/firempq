package queue_info

import (
	"io/ioutil"
	"path/filepath"

	"strings"

	"os"

	"strconv"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/ferr"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/utils"
)

func ConfigFilePath(path, serviceID string) string {
	return filepath.Join(path, serviceID+".conf")
}
func DescFilePath(path, serviceID string) string {
	return filepath.Join(path, serviceID+".desc")
}

func LoadServiceConfig(path string, serviceConfig apis.BinaryMarshaller) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return ferr.Wrapf(err, "failed to read service config: %s", path)
	}

	if err := serviceConfig.Unmarshal(data); err != nil {
		return ferr.Wrapf(err, "filed to decode service config")
	}
	return nil
}

// GetServiceDescriptions Loads all service descriptions prefixed with ServiceDescPrefix
func GetServiceDescription(path string) (*ServiceDescription, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, ferr.Wrapf(err, "failed to read svc description: %s", path)
	}
	desc, err := UnmarshalServiceDesc([]byte(data))
	if err != nil {
		return nil, ferr.Wrapf(err, "failed to decode svc description: %s", path)
	}

	return desc, nil
}

// SaveServiceConfig saves service config into database.
func SaveServiceConfig(path string, conf apis.BinaryMarshaller) error {
	data, _ := conf.Marshal()
	err := ioutil.WriteFile(path, data, 0600)
	if err != nil {
		return ferr.Wrapf(err, "failed to save service config: %s", path)
	}
	return nil
}

// SaveServiceDescription saves service config into database.
func SaveServiceDescription(path string, desc *ServiceDescription) error {
	data, _ := desc.Marshal()
	fp := DescFilePath(path, desc.ServiceId)
	err := ioutil.WriteFile(fp, data, 0600)
	if err != nil {
		return ferr.Wrapf(err, "failed to save service description: %s", fp)
	}
	return nil
}

// GetServiceDescriptions Loads all service descriptions prefixed with ServiceDescPrefix
func GetServiceDescriptions(path string) (ServiceDescriptionList, error) {
	sdList := make(ServiceDescriptionList, 0, 16)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, ferr.Wrapf(err, "failed to get list of service descriptions at %s", path)
	}

	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".desc") {
			continue
		}
		desc, err := GetServiceDescription(path)
		if err != nil {
			log.Error(err.Error())
		}
		sdList = append(sdList, desc)
	}

	return sdList, nil
}

func DeleteServiceData(path string, serviceId string) {
	fdesc := DescFilePath(path, serviceId)
	fconf := ConfigFilePath(path, serviceId)
	// TODO(vburenin): Handle errors.
	os.Remove(fdesc)
	os.Remove(fconf)
}

func NewServiceDescription(name string, exportId uint64) *ServiceDescription {
	return &ServiceDescription{
		ExportId:  exportId,
		SType:     "queue",
		Name:      name,
		CreateTs:  utils.Uts(),
		Disabled:  false,
		ToDelete:  false,
		ServiceId: strconv.FormatInt(int64(exportId), 10),
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
