package cldb

import (
	"os"
	"sync"

	"firempq/log"
)

var database *CLevelDBStorage
var lock sync.Mutex

// GetDatabase returns DataStorage singleton.
func GetDatabase() *CLevelDBStorage {
	lock.Lock()
	defer lock.Unlock()
	return getDatabase()
}

func getDatabase() *CLevelDBStorage {
	var err error
	if database == nil {
		database, err = NewLevelDBStorage("databasedir")
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
