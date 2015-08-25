package db

import (
	"sync"
)

var database *DataStorage
var lock sync.Mutex

// GetDatabase returns DataStorage singleton.
func GetDatabase() *DataStorage {
	lock.Lock()
	defer lock.Unlock()
	if database == nil {
		database = NewDataStorage("databasedir")
	}
	return database
}
