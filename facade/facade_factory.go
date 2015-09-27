package facade

import (
	"firempq/common"
	"firempq/db"
	"firempq/features/dsqueue"
	"firempq/features/pqueue"
	"sync"
)

type ServiceConstructor func(string, []string) common.ISvc
type ServiceLoader func(*db.DataStorage, string) (common.ISvc, error)

func GetServiceConstructor(serviceName string) (ServiceConstructor, bool) {
	switch serviceName {
	case common.STYPE_PRIORITY_QUEUE:
		return pqueue.CreatePQueue, true
	case common.STYPE_DOUBLE_SIDED_QUEUE:
		return dsqueue.CreateDSQueue, true
	default:
		return nil, false
	}
}

func GetServiceLoader(serviceName string) (ServiceLoader, bool) {
	switch serviceName {
	case common.STYPE_PRIORITY_QUEUE:
		return pqueue.LoadPQueue, true
	case common.STYPE_DOUBLE_SIDED_QUEUE:
		return dsqueue.LoadDSQueue, true
	default:
		return nil, false
	}
}

var facade *ServiceFacade
var lock sync.Mutex

func CreateFacade() *ServiceFacade {
	lock.Lock()
	defer lock.Unlock()
	if facade == nil {
		facade = NewFacade(db.GetDatabase())
	}
	return facade
}
