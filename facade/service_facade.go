package facade

import (
	"firempq/common"
	"firempq/db"
	"firempq/log"
	"strings"
	"sync"
)

type ServiceFacade struct {
	allSvcs  map[string]common.ISvc
	rwLock   sync.RWMutex
	database *db.DataStorage
}

func NewFacade(database *db.DataStorage) *ServiceFacade {
	f := ServiceFacade{
		database: database,
		allSvcs:  make(map[string]common.ISvc),
	}
	f.loadAllServices()
	return &f
}

func (s *ServiceFacade) loadAllServices() {
	for _, sm := range s.database.GetAllServiceMeta() {
		log.Info("Loading service data for: %s", sm.Name)
		serviceLoader, ok := GetServiceLoader(sm.SType)
		if !ok {
			log.Error("Unknown service '%s' type: %s", sm.Name, sm.SType)
			continue
		}
		svcInstance, err := serviceLoader(s.database, sm.Name)
		if err != nil {
			log.Error("Service '%s' was not loaded because of: %s", sm.Name, err)
		} else {
			s.allSvcs[sm.Name] = svcInstance
			svcInstance.StartUpdate()
		}
	}
}

func (s *ServiceFacade) CreateService(svcType string, svcName string, params []string) common.IResponse {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	if _, ok := s.allSvcs[svcName]; ok {
		return common.ERR_SVC_ALREADY_EXISTS
	}
	serviceConstructor, ok := GetServiceConstructor(svcType)
	if !ok {
		return common.ERR_SVC_UNKNOWN_TYPE
	}

	metaInfo := common.NewServiceMetaInfo(svcType, 0, svcName)
	s.database.SaveServiceMeta(metaInfo)
	svc := serviceConstructor(svcName, params)
	svc.StartUpdate()
	s.allSvcs[svcName] = svc

	return common.OK_RESPONSE
}

func (s *ServiceFacade) DropService(svcName string) common.IResponse {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	svc, ok := s.allSvcs[svcName]
	if !ok {
		return common.ERR_NO_SVC
	}
	svc.Close()
	delete(s.allSvcs, svcName)
	s.database.DeleteServiceData(svcName)
	return common.OK_RESPONSE
}

func (s *ServiceFacade) ListServices(svcPrefix string, svcType string) common.IResponse {

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

func (s *ServiceFacade) GetService(name string) (common.ISvc, bool) {
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
	s.database.FlushCache()
	s.database.Close()
}
