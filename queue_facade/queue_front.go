package queue_facade

import (
	"firempq/qerrors"
	"sync"
)

type IMessage interface {
	GetId() string
	GetStatus() map[string]interface{}
}

type IQueue interface {
	PushMessage(msgData map[string]string, payload []byte) error
	PopMessage() *IMessage
	DeleteById(msgId string) error
	GetStatus() map[string]interface{}
	ActionHandler(action string, params map[string]string) error
}

type PQFront struct {
	allQueues map[string]IQueue
	lock      sync.Mutex
}

func NewPQFront() *PQFront {
	return &PQFront{}
}

// All queues globally. Global things are terrible. Will move it out of here when design gets clear.
var ALL_QUEUES = NewPQFront()

func (p *PQFront) AddQueue(queueName string, pq IQueue) error {
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

func (p *PQFront) DropQueue(queueName string) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	_, ok := p.allQueues[queueName]
	if !ok {
		return qerrors.ERR_NO_QUEUE
	}
	delete(p.allQueues, queueName)
	return nil
}

func (p *PQFront) GetQueue(name string) (IQueue, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	q, ok := p.allQueues[name]
	if !ok {
		return nil, qerrors.ERR_NO_QUEUE
	}
	return q, nil
}
