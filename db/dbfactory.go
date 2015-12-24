package db

import (
	"firempq/db/ldb"
	"sync"

	. "firempq/api"
)

var database DataStorage = nil
var lock sync.Mutex

// GetDatabase returns DataStorage singleton.
func GetDatabase() DataStorage {
	lock.Lock()
	defer lock.Unlock()
	return getDatabase()
}

func SetDatabase(ds DataStorage) {
	database = ds
}

func getDatabase() DataStorage {
	if database == nil {
		database = ldb.GetDatabase()
	}
	if database.IsClosed() {
		database = nil
		return getDatabase()
	}
	return database
}
