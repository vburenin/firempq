package db

import (
	"os"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/db/ldb"
	"github.com/vburenin/firempq/log"
)

var database apis.DataStorage = nil

// GetDatabase returns DataStorage singleton.
func DatabaseInstance() apis.DataStorage {
	return getDatabase()
}

func SetDatabase(ds apis.DataStorage) {
	database = ds
}

func getDatabase() apis.DataStorage {
	if database == nil {
		var err error
		database, err = ldb.NewLevelDBStorage(conf.CFG)
		if err != nil {
			log.Error("Cannot initialize FireMPQ database: %s", err)
			os.Exit(255)
		}
		return database
	}

	if database.IsClosed() {
		database = nil
		return getDatabase()
	}
	return database
}
