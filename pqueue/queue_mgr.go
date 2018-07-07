package pqueue

import (
	"strings"
	"sync"

	"time"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/db"
	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/mpqproto"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/pmsg"
	"github.com/vburenin/firempq/queue_info"
)

type DeadMessage struct {
	msg    *pmsg.MsgMeta
	target string
}

type QueueManager struct {
	queues            map[string]*PQueue
	rwLock            sync.RWMutex
	queueIDsn         uint64
	db                apis.DataStorage
	cfg               *conf.Config
	deadMsgs          chan DeadMessage
	expireLoopBreaker chan struct{}

	wg sync.WaitGroup
}

func NewQueueManager(ctx *fctx.Context, db apis.DataStorage, config *conf.Config) *QueueManager {
	f := QueueManager{
		queues:            make(map[string]*PQueue),
		queueIDsn:         0,
		db:                db,
		cfg:               config,
		deadMsgs:          make(chan DeadMessage, 128),
		expireLoopBreaker: make(chan struct{}),
	}
	f.loadAllServices(ctx)
	f.wg.Add(2)
	go f.deadMessageProcessorLoop(fctx.Background("dead-queue"))
	go f.expireLoop(fctx.Background("expire-loop"))
	return &f
}

func (qm *QueueManager) deadMessageProcessorLoop(ctx *fctx.Context) {
	defer func() {
		qm.wg.Done()
		ctx.Info("stopped dead message processor queue")
	}()
	for {
		dm, ok := <-qm.deadMsgs
		if !ok {
			return
		}
		qm.rwLock.RLock()
		queue := qm.queues[dm.target]
		qm.rwLock.RUnlock()

		if queue != nil {
			queue.AddExistingMessage(dm.msg)
		}
	}
}

func (qm *QueueManager) expireLoop(ctx *fctx.Context) {
	defer func() {
		qm.wg.Done()
		ctx.Info("stopped expiration loop")
	}()
	for {
		qm.rwLock.RLock()
		processed := int64(0)
		for _, queue := range qm.queues {
			processed += queue.Update()
		}
		qm.rwLock.RUnlock()

		timeout := time.Duration(qm.cfg.UpdateInterval) * time.Millisecond
		if processed > 0 {
			timeout = time.Nanosecond
		}
		select {
		case <-time.After(timeout):
		case <-qm.expireLoopBreaker:
			return
		}
	}
}

func (qm *QueueManager) loadAllServices(ctx *fctx.Context) {
	descList, err := queue_info.GetServiceDescriptions(qm.cfg.DatabasePath)
	if err != nil {
		ctx.Fatalf("Failed to init: %s", err)
	}

	if len(descList) > 0 {
		qm.queueIDsn = descList[len(descList)-1].ExportId
	}

	queuesData := make(map[uint64]*QueueLoader)
	for _, desc := range descList {
		ctx.Debugf("found queue: %s", desc.Name)
		if _, ok := qm.queues[desc.Name]; ok {
			ctx.Warnf("Service with the same name detected: %s", desc.Name)
		}
		if desc.ToDelete {
			queue_info.DeleteServiceData(ctx, qm.cfg.DatabasePath, desc.Name)
		} else if desc.Disabled {
			ctx.Errorf("Service is disabled. Skipping: %s", desc.Name)
		} else {
			ctx.Debugf("Loading service data for: %s", desc.Name)
			cfgPath := queue_info.ConfigFilePath(qm.cfg.DatabasePath, desc.ServiceId)
			cfg := &conf.PQConfig{}
			err := queue_info.LoadServiceConfig(cfgPath, cfg)
			if err != nil {
				ctx.Errorf("Didn't decode config: %s", err)
				continue
			}
			qm.queues[desc.Name] = NewPQueue(qm.db, desc, qm.deadMsgs, cfg)
			queuesData[desc.ExportId] = NewQueueLoader()
		}
	}

	iter := db.NewIterator(ctx, qm.cfg.DatabasePath)

	err = ReplayData(ctx, queuesData, iter)
	if err != nil {
		ctx.Fatalf("Failed to restore queues state: %s", err)
	}

	for _, queue := range qm.queues {
		data := queuesData[queue.Description().ExportId]
		if data == nil {
			ctx.Fatalf("queue has disappeared: %s", queue.Description().Name)
		}
		queue.LoadMessages(data.Messages())
	}
}

// CreateService creates a service of the specified type.
func (qm *QueueManager) CreateQueueFromParams(ctx *fctx.Context, name string, params []string) apis.IResponse {
	pqConf, r := ParsePQConfig(params)
	if r.IsError() {
		ctx.Infof("Invalid service params for queue: %s", name)
		return r
	}
	return qm.CreateQueue(ctx, name, pqConf)
}

func (qm *QueueManager) CreateQueue(ctx *fctx.Context, queueName string, config *conf.PQConfig) apis.IResponse {
	qm.rwLock.Lock()
	defer qm.rwLock.Unlock()
	if !mpqproto.ValidateServiceName(queueName) {
		return mpqerr.ErrInvalidQueueName
	}
	if _, ok := qm.queues[queueName]; ok {
		return mpqerr.ErrSvcAlreadyExists
	}

	desc := queue_info.NewServiceDescription(queueName, qm.queueIDsn+1)
	svc := NewPQueue(qm.db, desc, qm.deadMsgs, config)

	if err := queue_info.SaveServiceDescription(qm.cfg.DatabasePath, desc); err != nil {
		ctx.Errorf("could not save service description: %s", err)
	}
	confPath := queue_info.ConfigFilePath(qm.cfg.DatabasePath, desc.ServiceId)
	if err := queue_info.SaveServiceConfig(confPath, config); err != nil {
		ctx.Errorf("could not save service config: %s", err)
	}

	qm.queueIDsn++
	qm.queues[queueName] = svc
	ctx.Infof("new queue has been created: %s", queueName)

	return resp.OK
}

// DropService drops service.
func (qm *QueueManager) DropService(ctx *fctx.Context, svcName string) apis.IResponse {
	qm.rwLock.Lock()
	defer qm.rwLock.Unlock()
	queue := qm.queues[svcName]
	if queue != nil {
		return mpqerr.ErrNoQueue
	}
	delete(qm.queues, svcName)
	svcID := queue.Description().ServiceId
	queue_info.DeleteServiceData(ctx, qm.cfg.DatabasePath, svcID)
	ctx.Infof("Service '%s' has been removed: (id:%s)", svcName, svcID)
	return resp.OK
}

func (qm *QueueManager) BuildServiceNameList(svcPrefix string) []string {
	var queues []string
	qm.rwLock.RLock()
	for queueName := range qm.queues {
		if strings.HasPrefix(queueName, svcPrefix) {
			queues = append(queues, queueName)
		}
	}
	qm.rwLock.RUnlock()
	return queues
}

// GetQueue look up of a service with appropriate name.
func (qm *QueueManager) GetQueue(name string) *PQueue {
	qm.rwLock.RLock()
	queue := qm.queues[name]
	qm.rwLock.RUnlock()
	return queue
}

// Close closes all available services walking through all of them.
func (qm *QueueManager) Close() {
	close(qm.expireLoopBreaker)
	close(qm.deadMsgs)
	qm.wg.Wait()
}
