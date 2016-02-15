package services

import (
	"firempq/log"
	"firempq/services/pqueue"
	"strings"
	"sync"

	. "firempq/api"
	. "firempq/common"
	"firempq/conf"
	. "firempq/errors"
	"firempq/parsers"
	. "firempq/response"
	. "firempq/services/svcmetadata"
)

type ServiceConstructor func(IServices, *ServiceDescription, []string) (ISvc, IResponse)
type ServiceLoader func(IServices, *ServiceDescription) (ISvc, error)

func GetServiceConstructor(serviceType string) (ServiceConstructor, bool) {
	switch serviceType {
	case STYPE_PRIORITY_QUEUE:
		return pqueue.CreatePQueue, true
	default:
		return nil, false
	}
}

func GetServiceLoader(serviceType string) (ServiceLoader, bool) {
	switch serviceType {
	case STYPE_PRIORITY_QUEUE:
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
	allSvcs          map[string]ISvc
	rwLock           sync.RWMutex
	serviceIdCounter uint64
}

func NewServiceManager() *ServiceManager {
	f := ServiceManager{
		allSvcs:          make(map[string]ISvc),
		serviceIdCounter: 0,
	}
	f.loadAllServices()
	return &f
}

func (s *ServiceManager) loadAllServices() {
	descList := GetServiceDescriptions()
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

func (s *ServiceManager) loadService(desc *ServiceDescription) (ISvc, bool) {
	if desc.Disabled {
		log.Error("Service is disabled. Skipping: %s", desc.Name)
		return nil, false
	}
	if desc.ToDelete {
		log.Warning("Service should be deleted: %s", desc.Name)
		DeleteServiceData(desc.Name)
		return nil, false
	}
	log.Info("Loading service data for: %s", desc.Name)

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
func (s *ServiceManager) CreateService(svcType string, svcName string, params []string) IResponse {
	switch svcType {
	case STYPE_PRIORITY_QUEUE:
		pqConf, resp := pqueue.ParsePQConfig(params)
		if resp.IsError() {
			return resp
		}
		return s.CreatePQueue(svcName, pqConf)
	default:
		return ERR_SVC_UNKNOWN_TYPE
	}
}

func (s *ServiceManager) CreatePQueue(svcName string, config *conf.PQConfig) IResponse {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	if !parsers.ValidateServiceName(svcName) {
		return ERR_WRONG_SVC_NAME
	}
	if _, ok := s.allSvcs[svcName]; ok {
		return ERR_SVC_ALREADY_EXISTS
	}

	desc := NewServiceDescription(svcName, STYPE_PRIORITY_QUEUE, s.serviceIdCounter+1)
	svc := pqueue.InitPQueue(s, desc, config)

	s.serviceIdCounter++
	SaveServiceDescription(desc)
	s.allSvcs[svcName] = svc

	svc.StartUpdate()

	return OK_RESPONSE
}

// DropService drops service.
func (s *ServiceManager) DropService(svcName string) IResponse {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	svc, ok := s.allSvcs[svcName]
	if !ok {
		return ERR_NO_SVC
	}
	svc.Close()
	delete(s.allSvcs, svcName)
	DeleteServiceData(svc.GetServiceId())
	log.Info("Service '%s' has been removed: (id:%s)", svcName, svc.GetServiceId())
	return OK_RESPONSE
}

// ListServiceNames returns a list of available
func (s *ServiceManager) ListServiceNames(svcPrefix string) IResponse {

	services := make([]string, 0)
	s.rwLock.RLock()
	for svcName, _ := range s.allSvcs {
		if svcPrefix == "?" || strings.HasPrefix(svcName, svcPrefix) {
			services = append(services, svcName)
		}
	}
	s.rwLock.RUnlock()

	return NewStrArrayResponse("+SVCLIST", services)
}

// GetService look up of a service with appropriate name.
func (s *ServiceManager) GetService(name string) (ISvc, bool) {
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
