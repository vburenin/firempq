package facade

import (
	"firempq/common"
	"firempq/features"
	"firempq/iface"
	"firempq/log"
	"strings"
	"sync"
)

type ServiceFacade struct {
	allSvcs          map[string]iface.ISvc
	rwLock           sync.RWMutex
	database         iface.DataStorage
	serviceIdCounter uint64
}

func NewFacade(database iface.DataStorage) *ServiceFacade {
	f := ServiceFacade{
		database:         database,
		allSvcs:          make(map[string]iface.ISvc),
		serviceIdCounter: 0,
	}
	f.loadAllServices()
	return &f
}

func (s *ServiceFacade) loadAllServices() {
	descList := features.GetServiceDescriptions()
	if len(descList) > 0 {
		s.serviceIdCounter = descList[len(descList)-1].ExportId
	}
	for _, desc := range descList {
		if desc.GetDisabled() {
			log.Error("Service is disabled. Skipping: %s", desc.Name)
			continue
		}
		if desc.GetToDelete() {
			log.Warning("Service should be deleted: %s", desc.GetName())
			features.DeleteServiceData(desc.GetName())
			continue
		}

		log.Info("Loading service data for: %s", desc.Name)
		serviceLoader, ok := GetServiceLoader(desc.SType)

		if !ok {
			log.Error("Unknown service '%s' type: %s", desc.Name, desc.SType)
			continue
		}
		svcInstance, err := serviceLoader(desc)
		if err != nil {
			log.Error("Service '%s' was not loaded because of: %s", desc.Name, err)
		} else {
			s.allSvcs[desc.Name] = svcInstance
			svcInstance.StartUpdate()
		}
	}
}

func (s *ServiceFacade) CreateService(svcType string, svcName string, params []string) iface.IResponse {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	if _, ok := s.allSvcs[svcName]; ok {
		return common.ERR_SVC_ALREADY_EXISTS
	}
	serviceConstructor, ok := GetServiceConstructor(svcType)
	if !ok {
		return common.ERR_SVC_UNKNOWN_TYPE
	}

	s.serviceIdCounter += 1

	desc := common.NewServiceDescription(svcType, s.serviceIdCounter, svcName)
	features.SaveServiceDescription(desc)
	svc := serviceConstructor(desc, params)
	svc.StartUpdate()
	s.allSvcs[svcName] = svc

	return common.OK_RESPONSE
}

func (s *ServiceFacade) DropService(svcName string) iface.IResponse {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	svc, ok := s.allSvcs[svcName]
	if !ok {
		return common.ERR_NO_SVC
	}
	svc.Close()
	delete(s.allSvcs, svcName)
	features.DeleteServiceData(svc.GetServiceId())
	log.Info("Service '%s' has been removed: (id:%s)", svcName, svc.GetServiceId())
	return common.OK_RESPONSE
}

func (s *ServiceFacade) ListServices(svcPrefix string, svcType string) iface.IResponse {

	if svcType != "" {
		_, ok := GetServiceConstructor(svcType)
		if !ok {
			return common.ERR_SVC_UNKNOWN_TYPE
		}
	}

	services := make([]string, 0)

	s.rwLock.RLock()
	for svcName, svc := range s.allSvcs {
		if svcType != "" && svcType != svc.GetTypeName() {
			continue
		}
		if svcPrefix == "?" || strings.HasPrefix(svcName, svcPrefix) {
			services = append(services, svcName+" "+svc.GetTypeName())
		}
	}
	s.rwLock.RUnlock()

	return common.NewStrArrayResponse(services)
}

func (s *ServiceFacade) GetService(name string) (iface.ISvc, bool) {
	s.rwLock.RLock()
	svc, ok := s.allSvcs[name]
	s.rwLock.RUnlock()
	return svc, ok
}

func (s *ServiceFacade) Close() {
	s.rwLock.Lock()
	for _, svc := range s.allSvcs {
		svc.Close()
	}
	s.rwLock.Unlock()
	s.database.Close()
}
