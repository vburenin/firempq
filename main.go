package main

import (
	"firempq/common"
	"firempq/defs"
	"firempq/facade"
	"firempq/features/pqueue"
	"firempq/server"
	"github.com/op/go-logging"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	//	"time"
)

var log = logging.MustGetLogger("firempq")

func init_logging() {
	format := logging.MustStringFormatter(
		"%{color}%{time:2006-01-02 15:04:05.00000}: %{level}%{color:reset} %{shortfile} %{message}",
	)
	logbackend := logging.NewLogBackend(os.Stderr, "", 0)
	formatter := logging.NewBackendFormatter(logbackend, format)
	logging.SetBackend(formatter)
	logging.SetLevel(logging.DEBUG, "firempq")
}

func main() {

	srv, err := server.GetServer(server.SIMPLE_SERVER, ":9033")
	if err != nil {
		log.Critical("Error: %s", err.Error())
	}

	srv.Start()
	//time.Sleep(1E9)
	//srv.Stop()
}

func addMessages(pq common.ISvc) {
	//	ts := time.Now().UnixNano()
	payload := "0000"
	//payload += payload
	//	payload += payload
	//	payload += payload
	//	payload += payload
	//	payload += payload
	//time.Sleep(60 * 1000000000)
	//pq.DeleteAll()
	v := map[string]string{
		defs.PRM_PRIORITY: "1",
		defs.PRM_PAYLOAD:  payload,
	}
	for i := 0; i < 1000000; i++ {
		pq.Call(pqueue.ACTION_PUSH, v)
	}
	//end_t := time.Now().UnixNano()

	//fmt.Println((end_t - ts) / 1000000)
}

func addSpeedTest(q common.ISvc) {

	addMessages(q)

}

func main1() {
	init_logging()
	f, _ := os.Create("pp.dat")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	runtime.GOMAXPROCS(runtime.NumCPU())

	fc := facade.CreateFacade()
	defer fc.Close()
	for i := 0; i < 1; i++ {
		qid := "tst_queue_" + strconv.Itoa(i)
		err := fc.CreateService(common.STYPE_PRIORITY_QUEUE, qid, nil)
		// err := fc.CreateQueue(common.QTYPE_PRIORITY_QUEUE, qid, nil)
		if err != nil {
			log.Notice("%s: %s", err.Error(), qid)
		}
	}
	log.Notice("Started")
	for i := 0; i < 1; i++ {
		qid := "tst_queue_" + strconv.Itoa(i)
		q, _ := fc.GetService(qid)
		addSpeedTest(q)
	}
	log.Notice("Finished")

}
