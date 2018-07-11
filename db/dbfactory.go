package db

import (
	"os"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/db/linear"
	"github.com/vburenin/firempq/fctx"
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

func NewIterator(ctx *fctx.Context, dbPath string) apis.ItemIterator {
	iter, err := linear.NewIterator(ctx, dbPath)
	if err != nil {
		ctx.Fatalf("Failed to create data iteration object: %s", err)
	}
	return iter
}

func getDatabase() apis.DataStorage {
	if database == nil {
		var err error
		database, err = linear.NewFlatStorage(conf.CFG.DatabasePath, 2000000000, 2000000000)
		if err != nil {
			log.Error("Cannot initialize FireMPQ database: %s", err)
			os.Exit(255)
		}
		return database
	}
	return database
}

func GetDatabase(path string) {

}
