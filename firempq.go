package main

import (
	"fmt"

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

	iface := fmt.Sprintf("%s:%d", conf.CFG.Interface, conf.CFG.Port)
	srv, err := server.Server(server.SimpleServerType, iface)
	if err != nil {
		log.Critical("Error: %s", err.Error())
		return
	}
	srv.Start()

}
