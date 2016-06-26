package main

import (
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/server"
)

func main() {
	// Initialize logging to a default INFO level to be able to log config error.
	log.InitLogging()

	err := conf.ReadConfig()
	if err != nil {
		log.Error(err.Error())
		return
	}
	// Reinitialize log level according to the config data.
	log.InitLogging()

	srv, err := server.Server(server.SimpleServerType, conf.CFG.FMPQServerInterface)
	if err != nil {
		log.Critical("Error: %s", err.Error())
		return
	}
	srv.Start()

}
