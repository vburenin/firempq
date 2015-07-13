package facade

import (
	"firempq/common"
	"firempq/db"
	"firempq/qerrors"
	"github.com/op/go-logging"
	"sync"
)

var log = logging.MustGetLogger("firempq")

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
	f.loadAllQueues()
	return &f
}

func (p *QFacade) loadAllQueues() {
	for _, qm := range p.database.GetAllQueueMeta() {
		log.Info("Loading queue data for: %s", qm.Name)
		objLoader, ok := QUEUE_LOADER[qm.Qtype]
		if !ok {
			log.Error("Unknown queue '%s' type: %s", qm.Name, qm.Qtype)
			continue
		}
		queueInstance, err := objLoader(p.database, qm.Name)
		if err != nil {
			log.Error("Queue '%s' was not loaded because of: %s", qm.Name, err)
		} else {
			p.allQueues[qm.Name] = queueInstance
		}
	}
}

func (p *QFacade) CreateQueue(queueType string, queueName string, queueParams map[string]string) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if _, ok := p.allQueues[queueName]; ok {
		return qerrors.ERR_QUEUE_ALREADY_EXISTS
	}
	queueCrt, ok := QUEUE_CREATER[queueType]
	if !ok {
		return qerrors.ERR_QUEUE_UNKNOWN_TYPE
	}
	qmeta := common.NewQueueMetaInfo(queueType, 0, queueName)
	p.database.SaveQueueMeta(qmeta)
	queue := queueCrt(queueName, queueParams)
	p.allQueues[queueName] = queue

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

func (p *QFacade) Close() {
	for _, q := range p.allQueues {
		q.Close()
	}
	p.database.FlushCache()
	p.database.Close()
}
