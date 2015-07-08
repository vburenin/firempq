package queue_factory

import (
	"firempq/factory/db_factory"
	"firempq/pqueue"
)

func GetPQueue(queueName string) *pqueue.PQueue {
	return pqueue.NewPQueue(db_factory.GetDatabase(), queueName, 100, 10000)
}
