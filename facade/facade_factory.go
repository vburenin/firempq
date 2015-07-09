package facade

import (
	"firempq/common"
	"firempq/db"
	"firempq/pqueue"
)

var QUEUE_REGISTRY = map[string](func(string, map[string]string) IQueue){
	common.QTYPE_PRIORITY_QUEUE: pqueue.CreatePQueue,
}

var facade QFacade = NewFacade(db.GetDatabase())

func CreateFacade() *QFacade {
	return facade
}
