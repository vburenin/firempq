package qmgr

import (
	"strings"
	"sync"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/db"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/mpqproto"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/queue_info"
)

var smgr *ServiceManager
var onceNewMgr sync.Once

func CreateServiceManager() *ServiceManager {
	onceNewMgr.Do(func() { smgr = NewServiceManager() })
	return smgr
}

type ServiceManager struct {
	allSvcs          map[string]apis.ISvc
	rwLock           sync.RWMutex
	serviceIdCounter uint64
}

func NewServiceManager() *ServiceManager {
	f := ServiceManager{
		allSvcs:          make(map[string]apis.ISvc),
		serviceIdCounter: 0,
	}
	f.loadAllServices()
	return &f
}

func (s *ServiceManager) loadAllServices() {
	descList, err := queue_info.GetServiceDescriptions(conf.CFG.DatabasePath)
	if err != nil {
		log.Fatal("Failed to init: %s", err)
	}

	if len(descList) > 0 {
		s.serviceIdCounter = descList[len(descList)-1].ExportId
	}
	for _, desc := range descList {
		if _, ok := s.allSvcs[desc.Name]; ok {
			log.Warning("Service with the same name detected: %s", desc.Name)
		}
		if svc, ok := s.loadService(desc); ok {
			s.allSvcs[desc.Name] = svc
		}
	}
	for _, svc := range s.allSvcs {
		svc.StartUpdate()
	}
}

func (s *ServiceManager) loadService(desc *queue_info.ServiceDescription) (apis.ISvc, bool) {
	if desc.Disabled {
		log.Error("Service is disabled. Skipping: %s", desc.Name)
		return nil, false
	}
	if desc.ToDelete {
		log.Warning("Service should be deleted: %s", desc.Name)
		queue_info.DeleteServiceData(conf.CFG.DatabasePath, desc.Name)
		return nil, false
	}
	log.Debug("Loading service data for: %s", desc.Name)

	cfgPath := queue_info.ConfigFilePath(conf.CFG.DatabasePath, desc.ServiceId)
	cfg := &conf.PQConfig{}
	err := queue_info.LoadServiceConfig(cfgPath, cfg)
	if err != nil {
		log.Error("Didn't decode config: %s", err)
		return nil, false
	}
	svcInstance := pqueue.NewPQueue(s, db.DatabaseInstance(), desc, cfg)
	return svcInstance, true
}

// CreateService creates a service of the specified type.
func (s *ServiceManager) CreateQueueFromParams(name string, params []string) apis.IResponse {
	pqConf, r := pqueue.ParsePQConfig(params)
	if r.IsError() {
		return r
	}
	return s.CreateQueue(name, pqConf)
}

func (s *ServiceManager) CreateQueue(svcName string, config *conf.PQConfig) apis.IResponse {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	if !mpqproto.ValidateServiceName(svcName) {
		return mpqerr.ERR_WRONG_SVC_NAME
	}
	if _, ok := s.allSvcs[svcName]; ok {
		return mpqerr.ERR_SVC_ALREADY_EXISTS
	}

	desc := queue_info.NewServiceDescription(svcName, s.serviceIdCounter+1)
	svc := pqueue.NewPQueue(s, db.DatabaseInstance(), desc, config)

	if err := queue_info.SaveServiceDescription(conf.CFG.DatabasePath, desc); err != nil {
		log.Error("could not save service description: %s", err)
	}
	confPath := queue_info.ConfigFilePath(conf.CFG.DatabasePath, desc.ServiceId)
	if err := queue_info.SaveServiceConfig(confPath, config); err != nil {
		log.Error("could not save service config: %s", err)
	}

	s.serviceIdCounter++
	s.allSvcs[svcName] = svc

	svc.StartUpdate()

	return resp.OK
}

// DropService drops service.
func (s *ServiceManager) DropService(svcName string) apis.IResponse {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	svc, ok := s.allSvcs[svcName]
	if !ok {
		return mpqerr.ERR_NO_SVC
	}
	svc.Close()
	delete(s.allSvcs, svcName)
	svcID := svc.Info().ID
	queue_info.DeleteServiceData(conf.CFG.DatabasePath, svcID)
	log.Debug("Service '%s' has been removed: (id:%s)", svcName, svcID)
	return resp.OK
}

func (s *ServiceManager) BuildServiceNameList(svcPrefix string) []string {
	services := make([]string, 0)
	s.rwLock.RLock()
	for svcName := range s.allSvcs {
		if strings.HasPrefix(svcName, svcPrefix) {
			services = append(services, svcName)
		}
	}
	s.rwLock.RUnlock()
	return services
}

// ListServiceNames returns a list of available
func (s *ServiceManager) ListServiceNames(svcPrefix string) apis.IResponse {
	return resp.NewStrArrayResponse("+SVCLIST", s.BuildServiceNameList(svcPrefix))
}

// GetService look up of a service with appropriate name.
func (s *ServiceManager) GetService(name string) (apis.ISvc, bool) {
	s.rwLock.RLock()
	svc, ok := s.allSvcs[name]
	s.rwLock.RUnlock()
	return svc, ok
}

// Close closes all available services walking through all of them.
func (s *ServiceManager) Close() {
	s.rwLock.Lock()
	for _, svc := range s.allSvcs {
		svc.Close()
	}
	s.rwLock.Unlock()
}
