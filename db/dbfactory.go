package db

import (
	"firempq/db/ldb"
	"firempq/log"
	"os"
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
		var err error
		database, err = ldb.NewLevelDBStorage("databasedir")
		if err != nil {
			log.Error("Cannot initialize FireMPQ database: %s", err)
			os.Exit(255)
		}
	}

	if database.IsClosed() {
		database = nil
		return getDatabase()
	}
	return database
}
