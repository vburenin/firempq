package factory

import "firempq/pqueue"

func GetPQueue(queueName string) *pqueue.PQueue {
	return pqueue.NewPQueue(GetDatabase(), queueName, 100, 10000)
}
