package pqueue

import (
	"firempq/common"
	"firempq/db"
)

func CreatePQueue(queueName string, params map[string]string) common.IQueue {
	return NewPQueue(db.GetDatabase(), queueName, 100, 1000)
}
