package main

import (
	"firempq/common"
	"firempq/config"
	"firempq/defs"
	"firempq/facade"
	"firempq/features/pqueue"
	"firempq/log"
	"firempq/server"
	"fmt"
	"strconv"
)

func main() {
	cfg := config.GetConfig()
	iface := fmt.Sprintf(":%d", cfg.Port)
	srv, err := server.GetServer(server.SIMPLE_SERVER, iface)
	if err != nil {
		log.Critical("Error: %s", err.Error())
	}

	srv.Start()
	//time.Sleep(1E9)
	//srv.Stop()
}

func addMessages(pq common.ISvc) {
	payload := "0000"
	v := []string{defs.PRM_PRIORITY, "1", defs.PRM_PAYLOAD, payload}
	for i := 0; i < 10000000; i++ {
		pq.Call(pqueue.ACTION_PUSH, v)
	}
}

func main1() {
	//	f, _ := os.Create("pp.dat")
	//	pprof.StartCPUProfile(f)
	//	defer pprof.StopCPUProfile()
	//
	//	runtime.GOMAXPROCS(runtime.NumCPU())

	fc := facade.CreateFacade()
	defer fc.Close()
	for i := 0; i < 1; i++ {
		qid := "tst_queue_" + strconv.Itoa(i)
		err := fc.CreateService(common.STYPE_PRIORITY_QUEUE, qid, nil)
		if err != nil {
			log.Notice("%s: %s", err, qid)
		}
	}
	start_ts := common.Uts()
	log.Notice("Started")
	for i := 0; i < 1; i++ {
		qid := "tst_queue_" + strconv.Itoa(i)
		q, _ := fc.GetService(qid)
		addMessages(q)
	}
	log.Notice("Finished. Elapsed: %d", common.Uts()-start_ts)
}
