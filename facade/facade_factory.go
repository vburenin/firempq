package facade

import (
	"firempq/common"
	"firempq/db"
	"firempq/features/pqueue"
	"firempq/features/dsqueue"
	"sync"
)

type CreateFactoryFunc func(string, map[string]string) common.IItemHandler
type DataLoaderFunc func(*db.DataStorage, string) (common.IItemHandler, error)

var QUEUE_CREATER = map[string](CreateFactoryFunc){
	common.QTYPE_PRIORITY_QUEUE: pqueue.CreatePQueue,
	common.QTYPE_DOUBLE_SIDED_QUEUE: dsqueue.CreateDSQueue,
}

var QUEUE_LOADER = map[string](DataLoaderFunc){
	common.QTYPE_PRIORITY_QUEUE: pqueue.LoadPQueue,
}

var facade *QFacade
var lock sync.Mutex

func CreateFacade() *QFacade {
	lock.Lock()
	defer lock.Unlock()
	if facade == nil {
		facade = NewFacade(db.GetDatabase())
	}
	return facade
}
