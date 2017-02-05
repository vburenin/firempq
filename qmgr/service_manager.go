package qmgr

import (
	"strings"
	"sync"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/mpqproto"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/queue_info"
)

type ServiceConstructor func(apis.IServices, *queue_info.ServiceDescription, []string) (apis.ISvc, apis.IResponse)
type ServiceLoader func(apis.IServices, *queue_info.ServiceDescription) (apis.ISvc, error)

func GetServiceLoader(serviceType string) (ServiceLoader, bool) {
	switch serviceType {
	case apis.ServiceTypePriorityQueue:
		return pqueue.LoadPQueue, true
	default:
		return nil, false
	}
}

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
	descList := queue_info.GetServiceDescriptions()
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
		queue_info.DeleteServiceData(desc.Name)
		return nil, false
	}
	log.Debug("Loading service data for: %s", desc.Name)

	serviceLoader, ok := GetServiceLoader(desc.SType)
	if !ok {
		log.Error("Unknown service '%s' type: %s", desc.Name, desc.SType)
		return nil, false
	}
	svcInstance, err := serviceLoader(s, desc)
	if err != nil {
		log.Error("Service '%s' was not loaded because of: %s", desc.Name, err)
		return nil, false
	}
	return svcInstance, true
}

// CreateService creates a service of the specified type.
func (s *ServiceManager) CreateService(svcType string, svcName string, params []string) apis.IResponse {
	switch svcType {
	case apis.ServiceTypePriorityQueue:
		pqConf, r := pqueue.ParsePQConfig(params)
		if r.IsError() {
			return r
		}
		return s.CreatePQueue(svcName, pqConf)
	default:
		return mpqerr.ERR_SVC_UNKNOWN_TYPE
	}
}

func (s *ServiceManager) CreatePQueue(svcName string, config *conf.PQConfig) apis.IResponse {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	if !mpqproto.ValidateServiceName(svcName) {
		return mpqerr.ERR_WRONG_SVC_NAME
	}
	if _, ok := s.allSvcs[svcName]; ok {
		return mpqerr.ERR_SVC_ALREADY_EXISTS
	}

	desc := queue_info.NewServiceDescription(svcName, apis.ServiceTypePriorityQueue, s.serviceIdCounter+1)
	svc := pqueue.InitPQueue(s, desc, config)

	s.serviceIdCounter++
	queue_info.SaveServiceDescription(desc)
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
	queue_info.DeleteServiceData(svcID)
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
