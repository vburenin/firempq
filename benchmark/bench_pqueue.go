package main

import (
	"firempq/common"
	"firempq/facade"
	"firempq/log"

	. "firempq/api"
	"firempq/db"
	. "firempq/features/pqueue"
	. "firempq/testutils"
	"sync"
	"time"
)

func BenchMassPush() {
	f := facade.NewFacade()
	resp := f.DropService("BenchTest")
	if !resp.IsError() {
		log.Info("BenchTest Queue exists alredy! Dropping...")
	}
	log.Info("Creating new service")
	resp = f.CreateService(common.STYPE_PRIORITY_QUEUE, "BenchTest", []string{})
	if resp.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	svc, ok := f.GetService("BenchTest")
	if !ok {
		log.Fatal("Could not get created service: BenchTest")
	}
	respWriter := NewTestResponseWriter()
	ctx := svc.NewContext(respWriter)

	ctx.Call(PQ_CMD_SET_PARAM, []string{CPRM_MAX_SIZE, "10000000", CPRM_MSG_TTL, "100"})

	startTs := time.Now().UnixNano()
	data := []string{PRM_PAYLOAD, "7777777777777777777777777777777777777777777777777777777777777777"}

	var grp sync.WaitGroup
	testFunc := func() {
		for i := 0; i < 1000000; i++ {
			ctx.Call(PQ_CMD_PUSH, data)
		}
		grp.Done()
	}
	log.Info("Adding goroutines")
	for i := 0; i < 10; i++ {
		grp.Add(1)
		go testFunc()

	}
	log.Info("Waiting for result")
	grp.Wait()
	ctx.Finish()

	finishTs := time.Now().UnixNano()
	log.Info("Test finished in: %d - %d", (finishTs - startTs), (finishTs-startTs)/1000000)
	db.GetDatabase().FlushCache()
	f.DropService("BenchTest")
	f.Close()
	db.GetDatabase().Close()
}

func BenchMassPushMultiQueue() {
	f := facade.NewFacade()
	resp1 := f.DropService("BenchTest1")
	resp2 := f.DropService("BenchTest2")
	resp3 := f.DropService("BenchTest3")

	resp1 = f.CreateService(common.STYPE_PRIORITY_QUEUE, "BenchTest1", []string{})
	if resp1.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	resp2 = f.CreateService(common.STYPE_PRIORITY_QUEUE, "BenchTest2", []string{})
	if resp2.IsError() {
		log.Fatal("Can not create BenchTest queue")
	}

	resp3 = f.CreateService(common.STYPE_PRIORITY_QUEUE, "BenchTest3", []string{})
	if resp3.IsError() {
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
	respWriter1 := NewTestResponseWriter()
	respWriter2 := NewTestResponseWriter()
	respWriter3 := NewTestResponseWriter()
	ctx1 := svc1.NewContext(respWriter1)
	ctx2 := svc2.NewContext(respWriter2)
	ctx3 := svc3.NewContext(respWriter3)

	ctx1.Call(PQ_CMD_SET_PARAM, []string{CPRM_MAX_SIZE, "10000000", CPRM_MSG_TTL, "100000", CPRM_DELIVERY_DELAY, "0"})
	ctx2.Call(PQ_CMD_SET_PARAM, []string{CPRM_MAX_SIZE, "10000000", CPRM_MSG_TTL, "100000", CPRM_DELIVERY_DELAY, "0"})
	ctx3.Call(PQ_CMD_SET_PARAM, []string{CPRM_MAX_SIZE, "10000000", CPRM_MSG_TTL, "100000", CPRM_DELIVERY_DELAY, "0"})

	startTs := time.Now().UnixNano()
	data := []string{PRM_PAYLOAD, "7777777777777777777777777777777777777777777777777777777777777777"}

	var grp sync.WaitGroup
	testFunc := func(ctx ServiceContext) {
		for i := 0; i < 1000000; i++ {
			ctx.Call(PQ_CMD_PUSH, data)
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

	}
	log.Info("Waiting for result")
	grp.Wait()
	ctx1.Finish()
	svc1.Close()
	ctx2.Finish()
	svc2.Close()
	ctx3.Finish()
	svc3.Close()

	finishTs := time.Now().UnixNano()
	log.Info("Test finished in: %d - %d", (finishTs - startTs), (finishTs-startTs)/1000000)

	db.GetDatabase().FlushCache()

	log.Info("Deleting BenchTest1")
	f.DropService("BenchTest1")
	log.Info("Deleting BenchTest2")
	f.DropService("BenchTest2")
	log.Info("Deleting BenchTest3")
	f.DropService("BenchTest3")
	log.Info("Closing facade...")
	f.Close()
	log.Info("Waiting flush...")
	log.Info("Closing DB")
	db.GetDatabase().Close()
}

func main() {
	log.InitLogging()
	BenchMassPush()
	//BenchMassPushMultiQueue()
}
