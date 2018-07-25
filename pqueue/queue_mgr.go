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
	"go.uber.org/zap"
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

func NewQueueManager(ctx *fctx.Context, config *conf.Config) (*QueueManager, error) {
	f := QueueManager{
		queues:            make(map[string]*PQueue),
		queueIDsn:         0,
		cfg:               config,
		deadMsgs:          make(chan DeadMessage, 128),
		expireLoopBreaker: make(chan struct{}),
		configMgr:         qconf.NewConfigManager(config.DatabasePath),
	}

	if err := f.loadAllServices(ctx); err != nil {
		return nil, err
	}
	f.wg.Add(2)
	go f.deadMessageProcessorLoop(fctx.Background("dead-queue"))
	go f.expireLoop(fctx.Background("expire-loop"))
	return &f, nil
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
				ctx.Error("failed to flush queue database", zap.String("queue", q.Description().Name))
			}
		}
		qm.rwLock.RUnlock()

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
		ctx.Error("queue is not initialized", zap.String("queue", desc.Name), zap.Error(err))
		return
	}

	ldb, err := db.GetDatabase(qm.configMgr.QueueDataPath(desc.ServiceId))
	if err != nil {
		ctx.Error("queue is not initialized", zap.String("queue", desc.Name), zap.Error(err))
		return
	}

	queue := NewPQueue(ldb, desc, cfg, qm.deadMsgs, qm.makeConfigUpdater(desc.ServiceId))
	queue.LoadMessages(ctx, ql.Messages())
	qm.rwLock.Lock()
	qm.queues[desc.Name] = queue
	qm.rwLock.Unlock()
}

func (qm *QueueManager) loadAllServices(ctx *fctx.Context) error {
	descList, err := qm.configMgr.LoadDescriptions()

	if err != nil {
		return err
	}

	if len(descList) > 0 {
		qm.queueIDsn = descList[len(descList)-1].ExportId
	}

	cwg := nsync.NewControlWaitGroup(8)
	for _, d := range descList {
		desc := d
		ctx.Debug("queue found", zap.String("queue", desc.Name))
		if _, ok := qm.queues[desc.Name]; ok {
			ctx.Warn("queue with the same name detected", zap.String("queue", desc.Name))
		}
		if desc.ToDelete {
			qm.configMgr.DeleteQueueData(ctx, desc.Name)
		} else if desc.Disabled {
			ctx.Warn("queue is disabled", zap.String("queue", desc.Name))
		} else {
			ctx.Debug("loading queue config", zap.String("queue", desc.Name))
			cfg, err := qm.configMgr.LoadConfig(desc.ServiceId)
			if err != nil {
				ctx.Error("failed to load queue config", zap.String("queue", desc.Name), zap.Error(err))
				continue
			}
			cwg.Do(func() { qm.initQueue(ctx, desc, cfg) })
		}
	}
	cwg.Wait()
	ctx.Debug("last queue integer id", zap.Uint64("exportID", qm.queueIDsn))
	return nil
}

// CreateService creates a service of the specified type.
func (qm *QueueManager) CreateQueueFromParams(ctx *fctx.Context, name string, params []string) apis.IResponse {
	pqConf, r := ParsePQConfig(params)
	if r.IsError() {
		ctx.Info("invalid queue parameters", zap.String("queue", name), zap.Strings("params", params))
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
		ctx.Error("queue is not created", zap.Error(err))
		return mpqerr.ErrDbProblem
	}

	ldb, err := db.GetDatabase(qm.configMgr.QueueDataPath(desc.ServiceId))
	if err != nil {
		ctx.Error("queue is not initialized", zap.String("queue", desc.Name), zap.Error(err))
		qm.configMgr.DeleteQueueData(ctx, desc.ServiceId)
		return mpqerr.ErrDbProblem
	}

	if err := qm.configMgr.SaveConfig(desc.ServiceId, config); err != nil {
		ctx.Error("queue config is not saved", zap.String("queue", desc.Name), zap.Error(err))
		qm.configMgr.DeleteQueueData(ctx, desc.ServiceId)
		return mpqerr.ErrDbProblem
	}

	svc := NewPQueue(ldb, desc, config, qm.deadMsgs, qm.makeConfigUpdater(desc.ServiceId))

	qm.queueIDsn++
	qm.queues[queueName] = svc
	ctx.Info("queue is created", zap.String("queue", desc.Name))

	return resp.OK
}

// DropQueue drops service.
func (qm *QueueManager) DropQueue(ctx *fctx.Context, queueName string) apis.IResponse {
	qm.rwLock.Lock()
	defer qm.rwLock.Unlock()
	queue := qm.queues[queueName]
	if queue == nil {
		return mpqerr.ErrNoQueue
	}
	err := queue.Close()
	if err != nil {
		ctx.Error("queue was not closed", zap.Error(err))
	}
	delete(qm.queues, queueName)
	svcID := queue.Description().ServiceId
	qm.configMgr.DeleteQueueData(ctx, svcID)
	ctx.Info("queue has been removed", zap.String("queue", queueName))
	return resp.OK
}

func (qm *QueueManager) GetQueueList(svcPrefix string) []string {
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
func (qm *QueueManager) Close(ctx *fctx.Context) {
	qm.rwLock.Lock()
	wg := sync.WaitGroup{}
	for _, q := range qm.queues {
		qq := q
		wg.Add(1)
		go func() {
			if err := qq.Close(); err != nil {
				ctx.Error("queue database not closed", zap.String("queue", q.Description().Name), zap.Error(err))
			}
			wg.Done()
		}()
	}
	wg.Wait()

	qm.rwLock.Unlock()
	close(qm.expireLoopBreaker)
	close(qm.deadMsgs)
	qm.wg.Wait()
}
