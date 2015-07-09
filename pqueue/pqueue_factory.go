package pqueue

import (
	"firempq/db"
    "firempq/common"
)

func CreatePQueue(queueName string, params map[string]string) common.IQueue {
	return NewPQueue(db.GetDatabase(), queueName, 10, 1000)
}
