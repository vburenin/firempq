package ldb

import (
	"os"
	"sync"

	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/log"
)

var database *LevelDBStorage
var lock sync.Mutex

// GetDatabase returns DataStorage singleton.
func GetDatabase() *LevelDBStorage {
	lock.Lock()
	defer lock.Unlock()
	return getDatabase()
}

func getDatabase() *LevelDBStorage {
	var err error
	if database == nil {
		database, err = NewLevelDBStorage("databasedir", conf.CFG)
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
