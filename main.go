package main

import "net"
import "log"
import (
    "firempq/proto"
    "fmt"
    "firempq/pqueue"
    "time"
    "runtime"
)
//import "time"
//import "firepmq/pqueue"
//import "strconv"
//import (
//	"os"
//	"runtime/pprof"
//)

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
    for i := 0; i < 5000000; i++ {
        msg := pqueue.NewPQMessage("test", 4)
        pq.PushMessage(msg)
    }
    end_t := time.Now().UnixNano()
    fmt.Println((end_t - ts) / 1000000)
}

func popAllMessages(pq *pqueue.PQueue) {
    ts := time.Now().UnixNano()
    for i := 0; i < 5000000; i++ {
        msg := pq.PopMessage()
        pq.RemoveLockedById(msg.Id)
    }
    end_t := time.Now().UnixNano()
    fmt.Println((end_t - ts) / 1000000)
}

func addSpeedTest() {

    pq := pqueue.NewPQueue(100, 10000)
    pq.PopLockTimeout = 6000
    addMessages(pq)
    popAllMessages(pq)
    runtime.GC()
    addMessages(pq)
    popAllMessages(pq)
    runtime.GC()
    addMessages(pq)
    popAllMessages(pq)
    runtime.GC()
    addMessages(pq)
    popAllMessages(pq)
    runtime.GC()
    addMessages(pq)
    popAllMessages(pq)
    runtime.GC()

    time.Sleep(200 * 1000000000)
}

func main() {
    addSpeedTest()
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