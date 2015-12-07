package db

import (
	"firempq/db/ldb"
	"firempq/log"
	"os"
	"sync"

	. "firempq/api"
)

var inmemory bool = false

func UseMemoryDB() {
	inmemory = true
}

func UseLevelDB() {
	inmemory = false
}

var database DataStorage = nil
var lock sync.Mutex

// GetDatabase returns DataStorage singleton.
func GetDatabase() DataStorage {
	lock.Lock()
	defer lock.Unlock()
	return getDatabase()
}

func getDatabase() DataStorage {
	var err error
	if database == nil {
		database, err = db.NewLevelDBStorage("databasedir")
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
