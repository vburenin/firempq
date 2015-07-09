package facade

import (
	"firempq/common"
	"firempq/db"
	"firempq/pqueue"
)

type QueueFactoryFunc func(string, map[string]string) common.IQueue

var QUEUE_REGISTRY = map[string](QueueFactoryFunc){
	common.QTYPE_PRIORITY_QUEUE: pqueue.CreatePQueue,
}

var facade *QFacade = NewFacade(db.GetDatabase())

func CreateFacade() *QFacade {
	return facade
}
