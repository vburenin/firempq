package facade

import (
	"firempq/common"
	"firempq/db"
	"firempq/qerrors"
	"sync"
)

type QFacade struct {
	allQueues map[string]common.IQueue
	lock      sync.Mutex
	database  *db.DataStorage
}

func NewFacade(database *db.DataStorage) *QFacade {
	f := QFacade{
		database:  database,
		allQueues: make(map[string]common.IQueue),
	}
	return &f
}

func (p *QFacade) loadAllQueues() {

}

func (p *QFacade) CreateQueue(queueType string, queueName string, queueParams map[string]string) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if _, ok := p.allQueues[queueName]; ok {
		return qerrors.ERR_QUEUE_ALREADY_EXISTS
	}
	queueCrt, ok := QUEUE_REGISTRY[queueType]
	if !ok {
		return qerrors.ERR_QUEUE_UNKNOWN_TYPE
	}
	p.allQueues[queueName] = queueCrt(queueName, queueParams)
	return nil
}

func (p *QFacade) DropQueue(queueName string) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	_, ok := p.allQueues[queueName]
	if !ok {
		return qerrors.ERR_NO_QUEUE
	}
	delete(p.allQueues, queueName)
	return nil
}

func (p *QFacade) GetQueue(name string) (common.IQueue, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	q, ok := p.allQueues[name]
	if !ok {
		return nil, qerrors.ERR_NO_QUEUE
	}
	return q, nil
}
