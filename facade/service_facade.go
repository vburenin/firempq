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
	lock     sync.Mutex
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
		objLoader, ok := SVC_LOADER[sm.SType]
		if !ok {
			log.Error("Unknown service '%s' type: %s", sm.Name, sm.SType)
			continue
		}
		svcInstance, err := objLoader(s.database, sm.Name)
		if err != nil {
			log.Error("Service '%s' was not loaded because of: %s", sm.Name, err)
		} else {
			s.allSvcs[sm.Name] = svcInstance
		}
	}
}

func (s *ServiceFacade) CreateService(svcType string, svcName string, params []string) common.IResponse {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.allSvcs[svcName]; ok {
		return common.ERR_SVC_ALREADY_EXISTS
	}
	svcCrt, ok := SVC_CREATOR[svcType]
	if !ok {
		return common.ERR_SVC_UNKNOWN_TYPE
	}

	metaInfo := common.NewServiceMetaInfo(svcType, 0, svcName)
	s.database.SaveServiceMeta(metaInfo)

	s.allSvcs[svcName] = svcCrt(svcName, params)

	return common.OK200_RESPONSE
}

func (s *ServiceFacade) DropService(svcName string) common.IResponse {
	s.lock.Lock()
	defer s.lock.Unlock()
	svc, ok := s.allSvcs[svcName]
	if !ok {
		return common.ERR_NO_SVC
	}
	svc.Close()
	delete(s.allSvcs, svcName)
	s.database.DeleteServiceData(svcName)
	return common.OK200_RESPONSE
}

func (s *ServiceFacade) ListServices(svcPrefix string, svcType string) common.IResponse {
	s.lock.Lock()
	defer s.lock.Unlock()

	if svcType != "" {
		_, ok := SVC_CREATOR[svcType]
		if !ok {
			return common.ERR_SVC_UNKNOWN_TYPE
		}
	}

	services := make([]string, 0)
	for svcName, svc := range s.allSvcs {
		if svcType != "" && svcType != svc.GetTypeName() {
			continue
		}
		if svcPrefix == "?" || strings.HasPrefix(svcName, svcPrefix) {
			services = append(services, svcName+" "+svc.GetTypeName())
		}
	}
	return common.NewStrArrayResponse(services)
}

func (s *ServiceFacade) GetService(name string) (common.ISvc, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	svc, ok := s.allSvcs[name]
	return svc, ok
}

func (s *ServiceFacade) Close() {
	for _, svc := range s.allSvcs {
		svc.Close()
	}
	s.database.FlushCache()
	s.database.Close()
}
