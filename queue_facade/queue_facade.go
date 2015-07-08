package queue_facade

import (
	"firempq/qerrors"
	"sync"
)

type IMessage interface {
	GetId() string
	GetStatus() map[string]interface{}
	ToBinary() []byte
}

type IQueue interface {
	PushMessage(msgData map[string]string, payload string) error
	PopMessage() (IMessage, error)
	GetMessagePayload(msgId string) string
	DeleteById(msgId string) error
	GetStatus() map[string]interface{}
	DeleteAll()
	GetQueueType() string
	CustomHandler(action string, params map[string]string) error
}

type PQFacade struct {
	allQueues map[string]IQueue
	lock      sync.Mutex
}

func NewPQFacade() *PQFacade {
	return &PQFacade{}
}

// All queues globally. Global things are terrible. Will move it out of here when design gets clear.
var ALL_QUEUES = NewPQFacade()

func (p *PQFacade) AddQueue(queueName string, pq IQueue) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	_, ok := p.allQueues[queueName]
	if ok {
		return qerrors.ERR_QUEUE_ALREADY_EXISTS
	} else {
		p.allQueues[queueName] = pq
	}
	return nil
}

func (p *PQFacade) DropQueue(queueName string) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	_, ok := p.allQueues[queueName]
	if !ok {
		return qerrors.ERR_NO_QUEUE
	}
	delete(p.allQueues, queueName)
	return nil
}

func (p *PQFacade) GetQueue(name string) (IQueue, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	q, ok := p.allQueues[name]
	if !ok {
		return nil, qerrors.ERR_NO_QUEUE
	}
	return q, nil
}
