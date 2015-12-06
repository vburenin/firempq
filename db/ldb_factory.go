package db

import (
	"os"
	"sync"

	"firempq/log"
)

var database *DataStorage
var lock sync.Mutex

// GetDatabase returns DataStorage singleton.
func GetDatabase() *DataStorage {
	lock.Lock()
	defer lock.Unlock()
	return getDatabase()
}

func getDatabase() *DataStorage {
	var err error
	if database == nil {
		database, err = NewDataStorage("databasedir")
		if err != nil {
			log.Error("Cannot initialize FireMPQ database: %s", err)
			os.Exit(255)
		}
	}
	if database.closed {
		database = nil
		return getDatabase()
	}
	return database
}
