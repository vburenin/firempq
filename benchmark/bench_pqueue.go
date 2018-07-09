package main

import (
	"fmt"
	"sync"
	"time"

	"os"

	"runtime/pprof"

	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/db"
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqtesting/resp_writer"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/qmgr"
)

func BenchMassPush() {
	conf.ParseConfigParameters()
	ctx := fctx.Background("1")
	f := qmgr.NewQueueManager(ctx)
	resp := f.DropService(ctx, "BenchTest")
	if !resp.IsError() {
		log.Info("BenchTest Queue exists alredy! Dropping...")
	}
	log.Info("Creating new service")
	resp = f.CreateQueueFromParams(ctx, "BenchTest", []string{})
	if resp.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	svc := f.GetQueue("BenchTest")
	if svc == nil {
		log.Fatal("Could not get created service: BenchTest")
	}
	respWriter := resp_writer.NewTestResponseWriter()
	cs := svc.ConnScope(respWriter)

	cs.Call(pqueue.CmdSetConfig, []string{pqueue.CPRM_MAX_MSGS_IN_QUEUE, "10000000", pqueue.CPRM_MSG_TTL, "10000000"})

	var grp sync.WaitGroup
	data := []string{pqueue.PrmPayload, "7777777777"}

	testFunc := func() {
		for i := 0; i < 1000000; i++ {
			cs.Call(pqueue.CmdPush, data)
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
	cs.Finish()

	log.Info("Test finished in: %d - %d", finishTs-startTs, (finishTs-startTs)/1000000)
	db.DatabaseInstance().Flush()
	//println("waiting...")
	//time.Sleep(time.Second * 1200)
	//f.DropService("BenchTest")
	f.Close()
	db.DatabaseInstance().Close()
}

func BenchMassPushMultiQueue() {
	conf.ParseConfigParameters()
	ctx := fctx.Background("1")
	f := qmgr.NewQueueManager(ctx)
	resp1 := f.DropService(ctx, "BenchTest1")
	resp2 := f.DropService(ctx, "BenchTest2")
	resp3 := f.DropService(ctx, "BenchTest3")
	resp4 := f.DropService(ctx, "BenchTest4")

	resp1 = f.CreateQueueFromParams(ctx, "BenchTest1", []string{})
	if resp1.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	resp2 = f.CreateQueueFromParams(ctx, "BenchTest2", []string{})
	if resp2.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	resp3 = f.CreateQueueFromParams(ctx, "BenchTest3", []string{})
	if resp3.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	resp4 = f.CreateQueueFromParams(ctx, "BenchTest4", []string{})
	if resp4.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	svc1 := f.GetQueue("BenchTest1")
	if svc1 == nil {
		log.Fatal("Could not get created service: BenchTest1")
	}
	svc2 := f.GetQueue("BenchTest2")
	if svc2 == nil {
		log.Fatal("Could not get created service: BenchTest2")
	}
	svc3 := f.GetQueue("BenchTest3")
	if svc3 == nil {
		log.Fatal("Could not get created service: BenchTest3")
	}

	svc4 := f.GetQueue("BenchTest4")
	if svc4 == nil {
		log.Fatal("Could not get created service: BenchTest4")
	}

	respWriter1 := resp_writer.NewTestResponseWriter()
	respWriter2 := resp_writer.NewTestResponseWriter()
	respWriter3 := resp_writer.NewTestResponseWriter()
	respWriter4 := resp_writer.NewTestResponseWriter()
	ctx1 := svc1.ConnScope(respWriter1)
	ctx2 := svc2.ConnScope(respWriter2)
	ctx3 := svc3.ConnScope(respWriter3)
	ctx4 := svc4.ConnScope(respWriter4)

	ctx1.Call(pqueue.CmdSetConfig, []string{pqueue.CPRM_MAX_MSGS_IN_QUEUE, "10000000", pqueue.CPRM_MSG_TTL, "100000", pqueue.CPRM_DELIVERY_DELAY, "0"})
	ctx2.Call(pqueue.CmdSetConfig, []string{pqueue.CPRM_MAX_MSGS_IN_QUEUE, "10000000", pqueue.CPRM_MSG_TTL, "100000", pqueue.CPRM_DELIVERY_DELAY, "0"})
	ctx3.Call(pqueue.CmdSetConfig, []string{pqueue.CPRM_MAX_MSGS_IN_QUEUE, "10000000", pqueue.CPRM_MSG_TTL, "100000", pqueue.CPRM_DELIVERY_DELAY, "0"})
	ctx4.Call(pqueue.CmdSetConfig, []string{pqueue.CPRM_MAX_MSGS_IN_QUEUE, "10000000", pqueue.CPRM_MSG_TTL, "100000", pqueue.CPRM_DELIVERY_DELAY, "0"})

	startTs := time.Now().UnixNano()
	data := []string{pqueue.PrmPayload, "777777777777"}

	var grp sync.WaitGroup
	testFunc := func(c *pqueue.ConnScope) {
		for i := 0; i < 1000000; i++ {
			c.Call(pqueue.CmdPush, data)
		}
		grp.Done()
	}
	log.Info("Adding goroutines")
	for i := 0; i < 10; i++ {
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

	db.DatabaseInstance().Flush()

	log.Info("Deleting BenchTest1")
	f.DropService(fctx.Background("1"), "BenchTest1")
	log.Info("Deleting BenchTest2")
	f.DropService(fctx.Background("1"), "BenchTest2")
	log.Info("Deleting BenchTest3")
	f.DropService(fctx.Background("1"), "BenchTest3")
	log.Info("Deleting BenchTest4")
	f.DropService(fctx.Background("1"), "BenchTest4")
	log.Info("Closing facade...")
	f.Close()
	log.Info("Waiting flush...")
	log.Info("Closing DB")
	db.DatabaseInstance().Close()
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
	f, err := os.Create("profile")
	if err != nil {
		log.Fatal("%s", err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	log.InitLogging()
	BenchMassPush()
	//BenchHeap()
	//BenchMassPushMultiQueue()
	//MarshalTest()
}
