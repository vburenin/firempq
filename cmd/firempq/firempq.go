package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"

	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/server"
	"go.uber.org/zap"
)

func main() {

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	// Initialize logging to a default INFO level to be able to log config error.
	log.InitLogging()
	conf.ParseConfigParameters()
	f, _ := os.Create("fmpq.profile")
	defer f.Close()

	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	//trace.Start(f)
	//defer trace.Stop()

	if len(conf.CFG.Profiler) > 0 {
		log.Info("initializing profiler")
		go func() {
			if err := http.ListenAndServe(conf.CFG.Profiler, nil); err != nil {
				log.Error("could not initialize profiler", zap.Error(err))
			}
		}()
	}

	// Reinitialize log level according to the config data.
	log.InitLogging()
	ctx := fctx.Background("start")
	server := server.NewServer(ctx)
	server.Start()
}
