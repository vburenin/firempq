package main

import (
	"firempq/common"
	"firempq/defs"
	"firempq/facade"
	"firempq/pqueue"
	"firempq/server"
	"github.com/op/go-logging"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"
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

func main1() {

	srv, err := server.GetServer(server.SIMPLE_SERVER, ":9033")
	if err != nil {
		log.Critical("Error: %s", err.Error())
	}

	go srv.Start()
	time.Sleep(1E9)
	srv.Stop()
}

func addMessages(pq common.IQueue) {
	//	ts := time.Now().UnixNano()
	payload := "0000"
	//payload += payload
	//	payload += payload
	//	payload += payload
	//	payload += payload
	//	payload += payload
	//time.Sleep(60 * 1000000000)
	//pq.DeleteAll()
	for i := 0; i < 10; i++ {
		v := map[string]string{
			defs.PARAM_MSG_PRIORITY: "1",
		}
		pq.PushMessage(v, payload)
	}
	//end_t := time.Now().UnixNano()

	//fmt.Println((end_t - ts) / 1000000)
}

func popAll(pq *pqueue.PQueue) {
	//ts := time.Now().UnixNano()
	for {
		msg, err := pq.PopMessage()
		if err != nil {
			break
		}
		pq.GetMessagePayload(msg.GetId())
		pq.DeleteLockedById(map[string]string{defs.PARAM_MSG_ID: msg.GetId()})
	}
	//end_t := time.Now().UnixNano()
	//fmt.Println((end_t - ts) / 1000000)
}

func addSpeedTest(q common.IQueue) {

	addMessages(q)

}

func main() {
	init_logging()
	f, _ := os.Create("pp.dat")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	runtime.GOMAXPROCS(runtime.NumCPU())

	fc := facade.CreateFacade()
	defer fc.Close()
	for i := 0; i < 4; i++ {
		qid := "tst_queue_" + strconv.Itoa(i)
		err := fc.CreateQueue(common.QTYPE_PRIORITY_QUEUE, qid, nil)
		if err != nil {
			log.Notice(err.Error())
		}
	}
	for i := 0; i < 4; i++ {
		qid := "tst_queue_" + strconv.Itoa(i)
		q, _ := fc.GetQueue(qid)
		addSpeedTest(q)
	}

}
