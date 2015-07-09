package pqueue

import (
	"firempq/db"
)

func CreatePQueue(queueName string, params map[string]string) *PQueue {
	return NewPQueue(db.GetDatabase(), queueName, 10, 1000)
}
