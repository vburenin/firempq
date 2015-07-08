package factory

import (
	"firempq/factory/db_factory"
	"firempq/factory/queue_factory"
	"firempq/factory/server_factory"
)

var GetServer = server_factory.GetServer
var GetPQueue = queue_factory.GetPQueue
var GetDatabase = db_factory.GetDatabase
