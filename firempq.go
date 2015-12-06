package main

import (
	"firempq/conf"
	"firempq/log"
	"firempq/server"
	"fmt"
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
	srv, err := server.GetServer(server.SIMPLE_SERVER, iface)
	if err != nil {
		log.Critical("Error: %s", err.Error())
		return
	}
	srv.Start()

}
