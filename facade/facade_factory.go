package facade

import (
	"firempq/common"
	"firempq/db"
	"firempq/features/dsqueue"
	"firempq/features/pqueue"
	"sync"
)

type CreateFactoryFunc func(string, []string) common.ISvc
type DataLoaderFunc func(*db.DataStorage, string) (common.ISvc, error)

var SVC_CREATOR = map[string](CreateFactoryFunc){
	common.STYPE_PRIORITY_QUEUE:     pqueue.CreatePQueue,
	common.STYPE_DOUBLE_SIDED_QUEUE: dsqueue.CreateDSQueue,
}

var SVC_LOADER = map[string](DataLoaderFunc){
	common.STYPE_PRIORITY_QUEUE:     pqueue.LoadPQueue,
	common.STYPE_DOUBLE_SIDED_QUEUE: dsqueue.LoadDSQueue,
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
