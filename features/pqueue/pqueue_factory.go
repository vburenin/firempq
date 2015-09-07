package pqueue

import (
	"firempq/common"
	"firempq/db"
)

func CreatePQueue(queueName string, params []string) common.ISvc {
	return NewPQueue(db.GetDatabase(), queueName, 100, 1000)
}
