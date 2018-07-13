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
	"github.com/vburenin/firempq/qconf"
	"github.com/vburenin/nsync"
)

type DeadMessage struct {
	msg    *pmsg.MsgMeta
	target string
}

type QueueManager struct {
	queues            map[string]*PQueue
	rwLock            sync.RWMutex
	queueIDsn         uint64
	cfg               *conf.Config
	deadMsgs          chan DeadMessage
	expireLoopBreaker chan struct{}
	configMgr         *qconf.ConfigManager

	wg sync.WaitGroup
}

func NewQueueManager(ctx *fctx.Context, config *conf.Config) *QueueManager {
	f := QueueManager{
		queues:            make(map[string]*PQueue),
		queueIDsn:         0,
		cfg:               config,
		deadMsgs:          make(chan DeadMessage, 128),
		expireLoopBreaker: make(chan struct{}),
		configMgr:         qconf.NewConfigManager(config.DatabasePath),
	}

	f.loadAllServices(ctx)
	f.wg.Add(2)
	go f.deadMessageProcessorLoop(fctx.Background("dead-queue"))
	go f.expireLoop(fctx.Background("expire-loop"))
	return &f
}

func (qm *QueueManager) flushLoop(ctx *fctx.Context) {
	defer func() {
		qm.wg.Done()
		ctx.Info("stopping flash loop")
	}()
	for {
		qm.rwLock.RLock()
		for _, q := range qm.queues {
			err := q.db.Flush()
			if err != nil {
				ctx.Errorf("flush failed for the queue %s: %s", q.Description().Name, err)
			}
		}
		qm.rwLock.Unlock()

		select {
		case <-time.After(time.Millisecond * 100):
		case <-qm.expireLoopBreaker:
			return
		}
	}
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

func (qm *QueueManager) initQueue(ctx *fctx.Context, desc *qconf.QueueDescription, cfg *qconf.QueueConfig) {
	ctx = fctx.WithParent(ctx, desc.Name)
	iterator := db.NewIterator(ctx, qm.configMgr.QueueDataPath(desc.ServiceId))

	ql := NewQueueLoader()
	if err := ql.ReplayData(ctx, iterator); err != nil {
		ctx.Errorf("Failed to initialize %s queue: %s", desc.Name, err)
		return
	}

	ldb, err := db.GetDatabase(qm.configMgr.QueueDataPath(desc.ServiceId))
	if err != nil {
		ctx.Errorf("Failed to initialize %s queue: %s", desc.Name, err)
		return
	}

	queue := NewPQueue(ldb, desc, cfg, qm.deadMsgs, qm.makeConfigUpdater(desc.ServiceId))
	queue.LoadMessages(ctx, ql.Messages())
	qm.rwLock.Lock()
	qm.queues[desc.Name] = queue
	qm.rwLock.Unlock()
}

func (qm *QueueManager) loadAllServices(ctx *fctx.Context) {
	descList, err := qm.configMgr.LoadDescriptions()
	if err != nil {
		ctx.Fatalf("Failed to init: %s", err)
	}

	if len(descList) > 0 {
		qm.queueIDsn = descList[len(descList)-1].ExportId
	}
	cwg := nsync.NewControlWaitGroup(8)
	for _, d := range descList {
		desc := d
		ctx.Debugf("found queue: %s", desc.Name)
		if _, ok := qm.queues[desc.Name]; ok {
			ctx.Warnf("Service with the same name detected: %s", desc.Name)
		}
		if desc.ToDelete {
			qm.configMgr.DeleteQueueData(ctx, desc.Name)
		} else if desc.Disabled {
			ctx.Errorf("Service is disabled. Skipping: %s", desc.Name)
		} else {
			ctx.Debugf("Loading service data for: %s", desc.Name)
			cfg, err := qm.configMgr.LoadConfig(desc.ServiceId)
			if err != nil {
				ctx.Errorf("Didn't load config: %s", err)
				continue
			}
			cwg.Do(func() { qm.initQueue(ctx, desc, cfg) })
		}
	}
	cwg.Wait()
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

func (qm *QueueManager) makeConfigUpdater(queueID string) func(*qconf.QueueParams) (*qconf.QueueConfig, error) {
	return func(config *qconf.QueueParams) (*qconf.QueueConfig, error) {
		return qm.configMgr.UpdateQueueConfig(queueID, config)
	}
}

func (qm *QueueManager) CreateQueue(ctx *fctx.Context, queueName string, config *qconf.QueueConfig) apis.IResponse {
	qm.rwLock.Lock()
	defer qm.rwLock.Unlock()
	if !mpqproto.ValidateServiceName(queueName) {
		return mpqerr.ErrInvalidQueueName
	}
	if _, ok := qm.queues[queueName]; ok {
		return mpqerr.ErrQueueAlreadyExists
	}

	desc, err := qm.configMgr.NewDescription(queueName, qm.queueIDsn+1)
	if err != nil {
		ctx.Errorf("could not create queue: %s", err)
		return mpqerr.ErrDbProblem
	}

	ldb, err := db.GetDatabase(qm.configMgr.QueueDataPath(desc.ServiceId))
	if err != nil {
		ctx.Errorf("Failed to initialize %s queue: %s", desc.Name, err)
		qm.configMgr.DeleteQueueData(ctx, desc.ServiceId)
		return mpqerr.ErrDbProblem
	}

	if err := qm.configMgr.SaveConfig(desc.ServiceId, config); err != nil {
		ctx.Errorf("could not save service config: %s", err)
		qm.configMgr.DeleteQueueData(ctx, desc.ServiceId)
		return mpqerr.ErrDbProblem
	}

	svc := NewPQueue(ldb, desc, config, qm.deadMsgs, qm.makeConfigUpdater(desc.ServiceId))

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
	qm.configMgr.DeleteQueueData(ctx, svcID)
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
	qm.rwLock.Lock()
	ctx := fctx.Background("shutdown")
	for _, q := range qm.queues {
		if err := q.Close(); err != nil {
			ctx.Errorf("Could not close database: %s", err)
		}
	}
	qm.rwLock.Unlock()
	close(qm.expireLoopBreaker)
	close(qm.deadMsgs)
	qm.wg.Wait()
}
