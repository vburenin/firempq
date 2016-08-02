package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"

	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/server"
)

func main() {
	// Initialize logging to a default INFO level to be able to log config error.

	cfg := flag.String("config", "firempq_cfg.json", "Configuration file used to run server")
	flag.Parse()

	go http.ListenAndServe(":5000", nil)
	log.InitLogging()

	err := conf.ReadConfig(*cfg)
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
