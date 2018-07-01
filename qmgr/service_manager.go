package qmgr

import (
	"strings"
	"sync"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/mpqproto"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/queue_info"
)

type QueueManager struct {
	allSvcs          map[string]*pqueue.PQueue
	rwLock           sync.RWMutex
	serviceIdCounter uint64
	db               apis.DataStorage
}

func NewServiceManager(ctx *fctx.Context, db apis.DataStorage) *QueueManager {
	f := QueueManager{
		allSvcs:          make(map[string]*pqueue.PQueue),
		serviceIdCounter: 0,
		db:               db,
	}
	f.loadAllServices(ctx)
	return &f
}

func (s *QueueManager) loadAllServices(ctx *fctx.Context) {

	descList, err := queue_info.GetServiceDescriptions(conf.CFG.DatabasePath)
	if err != nil {
		ctx.Fatalf("Failed to init: %s", err)
	}

	if len(descList) > 0 {
		s.serviceIdCounter = descList[len(descList)-1].ExportId
	}
	for _, desc := range descList {
		if _, ok := s.allSvcs[desc.Name]; ok {
			ctx.Warnf("Service with the same name detected: %s", desc.Name)
		}
		if svc, ok := s.loadService(ctx, desc); ok {
			s.allSvcs[desc.Name] = svc
		}
	}
	for _, svc := range s.allSvcs {
		svc.StartUpdate()
	}
}

func (s *QueueManager) loadService(ctx *fctx.Context, desc *queue_info.ServiceDescription) (*pqueue.PQueue, bool) {
	if desc.Disabled {
		ctx.Errorf("Service is disabled. Skipping: %s", desc.Name)
		return nil, false
	}
	if desc.ToDelete {
		ctx.Warnf("Service should be deleted: %s", desc.Name)
		queue_info.DeleteServiceData(ctx, conf.CFG.DatabasePath, desc.Name)
		return nil, false
	}
	ctx.Debugf("Loading service data for: %s", desc.Name)

	cfgPath := queue_info.ConfigFilePath(conf.CFG.DatabasePath, desc.ServiceId)
	cfg := &conf.PQConfig{}
	err := queue_info.LoadServiceConfig(cfgPath, cfg)
	if err != nil {
		ctx.Errorf("Didn't decode config: %s", err)
		return nil, false
	}
	svcInstance := pqueue.NewPQueue(s.GetQueue, s.db, desc, cfg)
	return svcInstance, true
}

// CreateService creates a service of the specified type.
func (s *QueueManager) CreateQueueFromParams(ctx *fctx.Context, name string, params []string) apis.IResponse {
	pqConf, r := pqueue.ParsePQConfig(params)
	if r.IsError() {
		ctx.Infof("Invalid service params for queue: %s", name)
		return r
	}
	return s.CreateQueue(ctx, name, pqConf)
}

func (s *QueueManager) CreateQueue(ctx *fctx.Context, svcName string, config *conf.PQConfig) apis.IResponse {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	if !mpqproto.ValidateServiceName(svcName) {
		return mpqerr.ERR_WRONG_SVC_NAME
	}
	if _, ok := s.allSvcs[svcName]; ok {
		return mpqerr.ERR_SVC_ALREADY_EXISTS
	}

	desc := queue_info.NewServiceDescription(svcName, s.serviceIdCounter+1)
	svc := pqueue.NewPQueue(s.GetQueue, s.db, desc, config)

	if err := queue_info.SaveServiceDescription(conf.CFG.DatabasePath, desc); err != nil {
		ctx.Errorf("could not save service description: %s", err)
	}
	confPath := queue_info.ConfigFilePath(conf.CFG.DatabasePath, desc.ServiceId)
	if err := queue_info.SaveServiceConfig(confPath, config); err != nil {
		ctx.Errorf("could not save service config: %s", err)
	}

	s.serviceIdCounter++
	s.allSvcs[svcName] = svc

	svc.StartUpdate()

	return resp.OK
}

// DropService drops service.
func (s *QueueManager) DropService(ctx *fctx.Context, svcName string) apis.IResponse {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	queue := s.allSvcs[svcName]
	if queue != nil {
		return mpqerr.ERR_NO_SVC
	}
	queue.Close()
	delete(s.allSvcs, svcName)
	svcID := queue.Description().ServiceId
	queue_info.DeleteServiceData(ctx, conf.CFG.DatabasePath, svcID)
	ctx.Infof("Service '%s' has been removed: (id:%s)", svcName, svcID)
	return resp.OK
}

func (s *QueueManager) BuildServiceNameList(svcPrefix string) []string {
	services := make([]string, 0)
	s.rwLock.RLock()
	for svcName := range s.allSvcs {
		if strings.HasPrefix(svcName, svcPrefix) {
			services = append(services, svcName)
		}
	}
	s.rwLock.RUnlock()
	return services
}

// ListServiceNames returns a list of available
func (s *QueueManager) ListServiceNames(svcPrefix string) apis.IResponse {
	return resp.NewStrArrayResponse("+SVCLIST", s.BuildServiceNameList(svcPrefix))
}

// GetQueue look up of a service with appropriate name.
func (s *QueueManager) GetQueue(name string) *pqueue.PQueue {
	s.rwLock.RLock()
	svc := s.allSvcs[name]
	s.rwLock.RUnlock()
	return svc
}

// Close closes all available services walking through all of them.
func (s *QueueManager) Close() {
	s.rwLock.Lock()
	for _, svc := range s.allSvcs {
		svc.Close()
	}
	s.rwLock.Unlock()
}
