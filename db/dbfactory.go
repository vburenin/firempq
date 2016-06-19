package db

import (
	"os"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/db/cldb"
	"github.com/vburenin/firempq/db/ldb"
	"github.com/vburenin/firempq/log"
)

var database apis.DataStorage = nil
var useGoLevelDB = false

// GetDatabase returns DataStorage singleton.
func GetDatabase() apis.DataStorage {
	return getDatabase()
}

func SetDatabase(ds apis.DataStorage) {
	database = ds
}

func getDatabase() apis.DataStorage {
	if database == nil {
		var err error
		if useGoLevelDB {
			database, err = ldb.NewLevelDBStorage("databasedir", conf.CFG)
		} else {
			database, err = cldb.NewLevelDBStorage("databasedir", conf.CFG)
		}
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
