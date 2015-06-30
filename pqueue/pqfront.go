package pqueue

//
//import (
//	"firepmq/qerrors"
//	"sync"
//)
//
//type PQFront struct {
//	pqueues map[string]interface{}
//	pqlock  sync.Mutex
//}
//
//func NewPQFront() *PQFront {
//	return &PQFront{}
//}
//
//func (p *PQFront) AddQueue(queueName string, pq interface{}) {
//	p.pqlock.Lock()
//	defer p.pqlock.Unlock()
//	queue, ok := p.pqueues[queueName]
//	if ok {
//		return qerrors.ERR_QUEUE_ALREADY_EXISTS
//	} else {
//		p.pqueues[queueName] = pq
//	}
//}
//
//func (p *PQFront) DropQueue(queueName string) error {
//	p.pqlock.Lock()
//	defer p.pqlock.Unlock()
//	queue, ok := p.pqueues[queueName]
//	if !ok {
//		return qerrors.ERR_NO_QUEUE
//	}
//	delete(p.pqueues, queueName)
//}
