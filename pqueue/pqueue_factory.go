package pqueue

import (
	"firempq/db"
)

func CreatePQueue(queueName string, params map[string]string) *PQueue {
	return NewPQueue(db.GetDatabase(), queueName, 100, 1000)
}
