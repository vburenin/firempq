package db

import (
	"sync"
)

var database *DataStorage = nil
var lock sync.Mutex

func GetDatabase() *DataStorage {
	lock.Lock()
	defer lock.Unlock()
	if database == nil {
		database = NewDataStorage("databasedir")
	}
	return database
}
