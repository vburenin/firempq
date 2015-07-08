package db_factory

import (
	"firempq/db"
	"sync"
)

var database *db.DataStorage = nil
var lock sync.Mutex

func GetDatabase() *db.DataStorage {
	lock.Lock()
	defer lock.Unlock()
	if database == nil {
		database = db.NewDataStorage("databasedir")
	}
	return database
}
