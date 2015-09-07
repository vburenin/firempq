package dsqueue

import (
	"firempq/common"
	"firempq/db"
)

func CreateDSQueue(queueName string, params []string) common.ISvc {
	return NewDSQueue(db.GetDatabase(), queueName, 1000)
}
