package dsqueue

import (
	"firempq/common"
	"firempq/db"
)

func CreateDSQueue(queueName string, params map[string]string) common.ISvc {
	return NewDSQueue(db.GetDatabase(), queueName, 1000)
}
