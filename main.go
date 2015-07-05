package main

import "net"
import "log"
import (
	"firempq/defs"
	"firempq/pqueue"
	"firempq/proto"
	"fmt"
	//	"strconv"
	"runtime"
	"time"
)

func main1() {

	ln, err := net.Listen("tcp", ":5001")
	if err != nil {
		log.Fatalln("Can't listen to 5001: %s", err.Error())
	}

	log.Println("Listening port 5001")
	for {
		conn, err := ln.Accept()
		if err != nil {
			//
		} else {
			go proto.ServeRequests(conn)
		}

	}
}

func addMessages(pq *pqueue.PQueue) {
	ts := time.Now().UnixNano()
	payload := "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"

	for i := 0; i < 100000; i++ {
		v := map[string]string{
			//defs.PARAM_MSG_ID:       strconv.Itoa(i),
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
	var pq *pqueue.PQueue

	pq = pqueue.NewPQueue(name, 100, 10000)
	addMessages(pq)
	pq.Close()

	//time.Sleep(1000000000)

	pq = pqueue.NewPQueue(name, 100, 10000)
	popAll(pq)
	pq.Close()

	pq = pqueue.NewPQueue(name, 100, 10000)
	popAll(pq)
	pq.Close()
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
	runtime.GOMAXPROCS(runtime.NumCPU())
	addSpeedTest("n1")
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
