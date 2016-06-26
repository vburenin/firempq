package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/db"
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqtesting"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/qmgr"
)

func BenchMassPush() {
	f := qmgr.NewServiceManager()
	resp := f.DropService("BenchTest")
	if !resp.IsError() {
		log.Info("BenchTest Queue exists alredy! Dropping...")
	}
	log.Info("Creating new service")
	resp = f.CreateService("pqueue", "BenchTest", []string{})
	if resp.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	svc, ok := f.GetService("BenchTest")
	if !ok {
		log.Fatal("Could not get created service: BenchTest")
	}
	respWriter := mpqtesting.NewTestResponseWriter()
	ctx := svc.NewContext(respWriter)

	ctx.Call(pqueue.PQ_CMD_SET_CFG, []string{pqueue.CPRM_MAX_MSGS_IN_QUEUE, "10000000", pqueue.CPRM_MSG_TTL, "10000000"})

	var grp sync.WaitGroup
	data := []string{pqueue.PRM_PAYLOAD, "7777777777777777777777777777777777777777777777777777777777777777"}

	testFunc := func() {
		for i := 0; i < 1000000; i++ {
			ctx.Call(pqueue.PQ_CMD_PUSH, data)
		}
		grp.Done()
	}
	log.Info("Adding goroutines")

	startTs := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		grp.Add(1)
		go testFunc()

	}
	log.Info("Waiting for result")
	grp.Wait()
	finishTs := time.Now().UnixNano()
	ctx.Finish()

	log.Info("Test finished in: %d - %d", (finishTs - startTs), (finishTs-startTs)/1000000)
	db.GetDatabase().FlushCache()
	//println("waiting...")
	//time.Sleep(time.Second * 1200)
	//f.DropService("BenchTest")
	f.Close()
	db.GetDatabase().Close()
}

func BenchMassPushMultiQueue() {
	f := qmgr.NewServiceManager()
	resp1 := f.DropService("BenchTest1")
	resp2 := f.DropService("BenchTest2")
	resp3 := f.DropService("BenchTest3")
	resp4 := f.DropService("BenchTest4")

	resp1 = f.CreateService(apis.STYPE_PRIORITY_QUEUE, "BenchTest1", []string{})
	if resp1.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	resp2 = f.CreateService(apis.STYPE_PRIORITY_QUEUE, "BenchTest2", []string{})
	if resp2.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	resp3 = f.CreateService(apis.STYPE_PRIORITY_QUEUE, "BenchTest3", []string{})
	if resp3.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	resp4 = f.CreateService(apis.STYPE_PRIORITY_QUEUE, "BenchTest4", []string{})
	if resp4.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	svc1, ok := f.GetService("BenchTest1")
	if !ok {
		log.Fatal("Could not get created service: BenchTest1")
	}
	svc2, ok := f.GetService("BenchTest2")
	if !ok {
		log.Fatal("Could not get created service: BenchTest2")
	}
	svc3, ok := f.GetService("BenchTest3")
	if !ok {
		log.Fatal("Could not get created service: BenchTest3")
	}

	svc4, ok := f.GetService("BenchTest4")
	if !ok {
		log.Fatal("Could not get created service: BenchTest4")
	}

	respWriter1 := mpqtesting.NewTestResponseWriter()
	respWriter2 := mpqtesting.NewTestResponseWriter()
	respWriter3 := mpqtesting.NewTestResponseWriter()
	respWriter4 := mpqtesting.NewTestResponseWriter()
	ctx1 := svc1.NewContext(respWriter1)
	ctx2 := svc2.NewContext(respWriter2)
	ctx3 := svc3.NewContext(respWriter3)
	ctx4 := svc4.NewContext(respWriter4)

	ctx1.Call(pqueue.PQ_CMD_SET_CFG, []string{pqueue.CPRM_MAX_MSGS_IN_QUEUE, "10000000", pqueue.CPRM_MSG_TTL, "100000", pqueue.CPRM_DELIVERY_DELAY, "0"})
	ctx2.Call(pqueue.PQ_CMD_SET_CFG, []string{pqueue.CPRM_MAX_MSGS_IN_QUEUE, "10000000", pqueue.CPRM_MSG_TTL, "100000", pqueue.CPRM_DELIVERY_DELAY, "0"})
	ctx3.Call(pqueue.PQ_CMD_SET_CFG, []string{pqueue.CPRM_MAX_MSGS_IN_QUEUE, "10000000", pqueue.CPRM_MSG_TTL, "100000", pqueue.CPRM_DELIVERY_DELAY, "0"})
	ctx4.Call(pqueue.PQ_CMD_SET_CFG, []string{pqueue.CPRM_MAX_MSGS_IN_QUEUE, "10000000", pqueue.CPRM_MSG_TTL, "100000", pqueue.CPRM_DELIVERY_DELAY, "0"})

	startTs := time.Now().UnixNano()
	data := []string{pqueue.PRM_PAYLOAD, "7777777777777777777777777777777777777777777777777777777777777777"}

	var grp sync.WaitGroup
	testFunc := func(ctx apis.ServiceContext) {
		for i := 0; i < 1000000; i++ {
			ctx.Call(pqueue.PQ_CMD_PUSH, data)
		}
		grp.Done()
	}
	log.Info("Adding goroutines")
	for i := 0; i < 1; i++ {
		grp.Add(1)
		go testFunc(ctx1)
		grp.Add(1)
		go testFunc(ctx2)
		grp.Add(1)
		go testFunc(ctx3)
		grp.Add(1)
		go testFunc(ctx4)
	}
	log.Info("Waiting for result")
	grp.Wait()
	ctx1.Finish()
	svc1.Close()
	ctx2.Finish()
	svc2.Close()
	ctx3.Finish()
	svc3.Close()

	ctx4.Finish()
	svc4.Close()

	finishTs := time.Now().UnixNano()
	log.Info("Test finished in: %d - %d", (finishTs - startTs), (finishTs-startTs)/1000000)

	db.GetDatabase().FlushCache()

	log.Info("Deleting BenchTest1")
	f.DropService("BenchTest1")
	log.Info("Deleting BenchTest2")
	f.DropService("BenchTest2")
	log.Info("Deleting BenchTest3")
	f.DropService("BenchTest3")
	log.Info("Deleting BenchTest4")
	f.DropService("BenchTest4")
	log.Info("Closing facade...")
	f.Close()
	log.Info("Waiting flush...")
	log.Info("Closing DB")
	db.GetDatabase().Close()
}

func sn2db(v uint64) string {

	b := make([]byte, 8)
	b[0] = uint8(v >> 56)
	b[1] = uint8(v >> 48)
	b[2] = uint8(v >> 40)
	b[3] = uint8(v >> 32)
	b[4] = uint8(v >> 24)
	b[5] = uint8(v >> 16)
	b[6] = uint8(v >> 8)
	b[7] = uint8(v)
	return enc.UnsafeBytesToString(b)
}

func MarshalTest() {
	//m := NewPQMsgMetaData("ereteteyeueueueueueueueu", 10, 123456789023, 1234)
	st := time.Now().UnixNano()
	var i uint64
	for i = 0; i < 200000000; i++ {
		b := make([]byte, 8)
		b[0] = uint8(i >> 56)
		b[1] = uint8(i >> 48)
		b[2] = uint8(i >> 40)
		b[3] = uint8(i >> 32)
		b[4] = uint8(i >> 24)
		b[5] = uint8(i >> 16)
		b[6] = uint8(i >> 8)
		b[7] = uint8(i)
		enc.UnsafeBytesToString(b)
	}
	ft := time.Now().UnixNano() - st
	fmt.Println("Data prepared in", float64(ft)/1000000, "ms")
}

func main() {
	log.InitLogging()
	BenchMassPush()
	//BenchHeap()
	//BenchMassPushMultiQueue()
	//MarshalTest()
}
