package db

import (
	"os"
	"sync"
)

var database *DataStorage
var lock sync.Mutex

// GetDatabase returns DataStorage singleton.
func GetDatabase() *DataStorage {
	lock.Lock()
	var err error
	defer lock.Unlock()
	if database == nil {
		database, err = NewDataStorage("databasedir")
		if err != nil {
			log.Error("Cannot initialize FireMPQ database: %s", err)
			os.Exit(255)
		}
	}
	return database
}
