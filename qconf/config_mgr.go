package qconf

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/ferr"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/utils"
)

type ConfigManager struct {
	dbPath string
	mu     sync.Mutex
}

func NewConfigManager(dbPath string) *ConfigManager {
	return &ConfigManager{dbPath: dbPath}
}

func (qcm *ConfigManager) ConfigFilePath(queueID string) string {
	return filepath.Join(qcm.dbPath, queueID, "config")
}
func (qcm *ConfigManager) DescriptionFilePath(queueID string) string {
	return filepath.Join(qcm.dbPath, queueID, "description")
}

func (qcm *ConfigManager) LoadConfig(queueID string) (*QueueConfig, error) {
	fp := qcm.ConfigFilePath(queueID)
	data, err := ioutil.ReadFile(fp)
	if err != nil {
		return nil, ferr.Wrapf(err, "failed to read service config: %s", fp)
	}

	qc := &QueueConfig{}
	if err := qc.Unmarshal(data); err != nil {
		return nil, ferr.Wrapf(err, "filed to decode service config")
	}
	return qc, nil
}

// LoadDescriptions Loads all service descriptions prefixed with ServiceDescPrefix
func (qcm *ConfigManager) GetDescription(queueID string) (*QueueDescription, error) {
	fp := qcm.DescriptionFilePath(queueID)
	data, err := ioutil.ReadFile(fp)
	if err != nil {
		return nil, ferr.Wrapf(err, "failed to read svc description: %s", fp)
	}
	desc := &QueueDescription{}
	err = desc.Unmarshal([]byte(data))
	if err != nil {
		return nil, ferr.Wrapf(err, "failed to decode svc description: %s", fp)
	}

	return desc, nil
}

// SaveConfig saves service config into database.
func (qcm *ConfigManager) SaveConfig(queueID string, config *QueueConfig) error {
	fp := qcm.ConfigFilePath(queueID)
	data, _ := config.Marshal()
	err := ioutil.WriteFile(fp, data, 0600)
	if err != nil {
		return ferr.Wrapf(err, "failed to save service config: %s", fp)
	}
	return nil
}

// SaveDescription saves service config into database.
func (qcm *ConfigManager) SaveDescription(queueID string, desc *QueueDescription) error {
	data, _ := desc.Marshal()
	fp := qcm.DescriptionFilePath(queueID)
	err := ioutil.WriteFile(fp, data, 0600)
	if err != nil {
		return ferr.Wrapf(err, "failed to save service description: %s", fp)
	}
	return nil
}

// LoadDescriptions Loads all service descriptions prefixed with ServiceDescPrefix
func (qcm *ConfigManager) LoadDescriptions() (ServiceDescriptionList, error) {

	sdList := make(ServiceDescriptionList, 0, 16)
	files, err := ioutil.ReadDir(qcm.dbPath)
	if err != nil {
		return nil, ferr.Wrapf(err, "failed to get list of service descriptions at %s", qcm.dbPath)
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		info, err := qcm.GetDescription(f.Name())
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			log.Error(err.Error())
			continue
		}
		sdList = append(sdList, info)
	}

	return sdList, nil
}

type QueueParams struct {
	MsgTTL         *int64
	MaxMsgSize     *int64
	MaxMsgsInQueue *int64
	DeliveryDelay  *int64
	PopCountLimit  *int64
	PopLockTimeout *int64
	PopWaitTimeout *int64
	FailQueue      *string
}

func (qcm *ConfigManager) UpdateQueueConfig(queueID string, params *QueueParams) (*QueueConfig, error) {
	qcm.mu.Lock()
	defer qcm.mu.Unlock()
	conf, err := qcm.LoadConfig(queueID)
	if err != nil {
		return nil, err
	}

	if params.FailQueue != nil {
		conf.PopLimitQueueName = *params.FailQueue
	}

	conf.LastUpdateTs = utils.Uts()

	if params.MsgTTL != nil {
		conf.MsgTtl = *params.MsgTTL
	}
	if params.MaxMsgSize != nil {
		conf.MaxMsgSize = *params.MaxMsgSize
	}
	if params.MaxMsgsInQueue != nil {
		conf.MaxMsgsInQueue = *params.MaxMsgsInQueue
	}
	if params.DeliveryDelay != nil {
		conf.DeliveryDelay = *params.DeliveryDelay
	}
	if params.PopCountLimit != nil {
		conf.PopCountLimit = *params.PopCountLimit
	}
	if params.PopLockTimeout != nil {
		conf.PopLockTimeout = *params.PopLockTimeout
	}
	if params.PopWaitTimeout != nil {
		conf.PopWaitTimeout = *params.PopWaitTimeout
	}
	err = qcm.SaveConfig(queueID, conf)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (qcm *ConfigManager) DeleteQueueData(ctx *fctx.Context, queueID string) error {
	qcm.mu.Lock()
	defer qcm.mu.Unlock()

	fp := filepath.Join(qcm.dbPath, queueID)
	err := os.RemoveAll(fp)
	if err != nil {
		return ferr.Wrapf(err, "could not remove queue data at: %s", fp)
	}
	return nil
}

func (qcm *ConfigManager) NewDescription(name string, exportId uint64) (*QueueDescription, error) {
	svcID := strconv.FormatInt(int64(exportId), 10)
	desc := &QueueDescription{
		ExportId:  exportId,
		SType:     "queue",
		Name:      name,
		CreateTs:  utils.Uts(),
		Disabled:  false,
		ToDelete:  false,
		ServiceId: svcID,
	}
	path := filepath.Join(qcm.dbPath, svcID)
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, ferr.Wrapf(err, "could not create queue directory: %s", path)
	}

	err = qcm.SaveDescription(svcID, desc)
	if err != nil {
		return nil, err
	}
	return desc, nil
}

type ServiceDescriptionList []*QueueDescription

func (p ServiceDescriptionList) Len() int           { return len(p) }
func (p ServiceDescriptionList) Less(i, j int) bool { return p[i].ExportId < p[j].ExportId }
func (p ServiceDescriptionList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
