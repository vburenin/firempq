package facade

import (
	"firempq/common"
	"firempq/features/pqueue"
	"sync"

	. "firempq/api"
)

type ServiceConstructor func(*common.ServiceDescription, []string) (ISvc, IResponse)
type ServiceLoader func(*common.ServiceDescription) (ISvc, error)

func GetServiceConstructor(serviceType string) (ServiceConstructor, bool) {
	switch serviceType {
	case common.STYPE_PRIORITY_QUEUE:
		return pqueue.CreatePQueue, true
	default:
		return nil, false
	}
}

func GetServiceLoader(serviceType string) (ServiceLoader, bool) {
	switch serviceType {
	case common.STYPE_PRIORITY_QUEUE:
		return pqueue.LoadPQueue, true
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
		facade = NewFacade()
	}
	return facade
}
