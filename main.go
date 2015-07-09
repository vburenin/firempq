package main

import "log"
import (
	"firempq/common"
	"firempq/db"
	"firempq/defs"
	"firempq/pqueue"
	"firempq/server"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

func main1() {

	srv, err := server.GetServer(server.SIMPLE_SERVER, ":9033")
	if err != nil {
		log.Fatalln("Error: %s", err.Error())
	}

	go srv.Start()
	time.Sleep(1E9)
	srv.Stop()
}

func addMessages(pq common.IQueue) {
	ts := time.Now().UnixNano()
	payload := "0000"
	//payload += payload
	//	payload += payload
	//	payload += payload
	//	payload += payload
	//	payload += payload
	//time.Sleep(60 * 1000000000)
	//pq.DeleteAll()
	for i := 0; i < 200; i++ {
		v := map[string]string{
			defs.PARAM_MSG_PRIORITY: "1",
		}
		pq.PushMessage(v, payload)
	}
	end_t := time.Now().UnixNano()

	fmt.Println((end_t - ts) / 1000000)
}

func popAll(pq *pqueue.PQueue) {
	ts := time.Now().UnixNano()
	for {
		msg, err := pq.PopMessage()
		if err != nil {
			break
		}
		pq.GetMessagePayload(msg.GetId())
		pq.DeleteLockedById(map[string]string{defs.PARAM_MSG_ID: msg.GetId()})
	}
	end_t := time.Now().UnixNano()
	fmt.Println((end_t - ts) / 1000000)
}

func addSpeedTest(name string) {

	var pq common.IQueue = pqueue.CreatePQueue("somepqueue", nil)
	addMessages(pq)

	msgq, err := pq.PopWait(1, 104)
	if err != nil || len(msgq) == 0 {
		fmt.Printf("PopWait returned empty msgq\n", len(msgq))
		return
	} else {
		fmt.Printf("PopWait done with %v messages\n", len(msgq))
	}
	pq.Close()

	//	pq = pqueue.NewPQueue(name, 100, 10000)
	//	popAll(pq)
	//	pq.Close()
	//
	//	//time.Sleep(50000000000)
	//
	//	pq = pqueue.NewPQueue(name, 100, 10000)
	//	popAll(pq)
	//	pq.Close()
	//	go addMessages(pq)
	//	go addMessages(pq)
	//	go addMessages(pq)
	//	go addMessages(pq)
	//	go addMessages(pq)
	//	go addMessages(pq)
	//	go addMessages(pq)
	//	go addMessages(pq)
	//	go addMessages(pq)
	//	go addMessages(pq)
	//	time.Sleep(200000000000)
	// msg := pq.Pop()
	// fmt.Println(msg)
	//fmt.Println(pq.GetMessagePayload(msg.GetId()))
}

func main() {

	f, _ := os.Create("pp.dat")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	defer db.GetDatabase().Close()

	runtime.GOMAXPROCS(runtime.NumCPU())
	addSpeedTest("tstq")

	//db := factory.GetDatabase()
	//	qmi1 := common.NewQueueMetaInfo(common.QTYPE_PRIORITY_QUEUE, 1, "test1")
	//	qmi2 := common.NewQueueMetaInfo(common.QTYPE_PRIORITY_QUEUE, 2, "test2")
	//	qmi3 := common.NewQueueMetaInfo(common.QTYPE_PRIORITY_QUEUE, 3, "test3")
	//	qmi4 := common.NewQueueMetaInfo(common.QTYPE_PRIORITY_QUEUE, 4, "test4")
	//	db.SaveQueueMeta(qmi1)
	//	db.SaveQueueMeta(qmi2)
	//	db.SaveQueueMeta(qmi3)
	//	db.SaveQueueMeta(qmi4)

	//	for _, v := range db.GetAllQueueMeta() {
	//		fmt.Println(v)
	//	}
	//db.Close()
	//	ts := time.Now().UnixNano()
	//	addSpeedTest("n1")
	//	end_t := time.Now().UnixNano()
	//	fmt.Println((end_t - ts) / 1000000)
	// println(util.GenRandMsgId())
	//testldb()
	// addSpeedTest()
	//    pq := pqueue.NewPQueue(100, 10000)
	//    pq.MsgTTL = 5000
	//    pq.PopLockTimeout = 2000
	//
	//    msg1 := pqueue.NewPQMessage("data1", 2)
	//    msg2 := pqueue.NewPQMessage("data2", 3)
	//    msg3 := pqueue.NewPQMessage("data3", 3)
	//
	//    pq.PushMessage(msg1)
	//    pq.PushMessage(msg2)
	//    pq.PushMessage(msg3)
	//
	//    fmt.Println(pq.PopMessage())
	//    fmt.Println(pq.PopMessage())
	//    fmt.Println(pq.PopMessage())
	//    pq.SetLockTimeout(msg1.Id, 3000)
	//
	//    log.Println("Three messages added")
	//
	//    time.Sleep(10 * 1000000000)
	//    fmt.Println("Done")

}
