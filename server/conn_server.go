package server

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/db"
	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/snsproto"
	"github.com/vburenin/firempq/server/sqsproto"
	"github.com/vburenin/firempq/signals"
	"go.uber.org/zap"
	"gopkg.in/tylerb/graceful.v1"
)

type QueueOpFunc func(req []string) error

type ConnectionServer struct {
	qmgr       *pqueue.QueueManager
	signalChan chan os.Signal
	waitGroup  sync.WaitGroup
}

func NewServer(ctx *fctx.Context) *ConnectionServer {
	os.MkdirAll(conf.CFG.DatabasePath, 0700)
	mgr, err := pqueue.NewQueueManager(ctx, conf.CFG)
	if err != nil {
		log.Fatal("queue manager is not initialized", zap.Error(err))
	}
	return &ConnectionServer{
		qmgr:       mgr,
		signalChan: make(chan os.Signal, 1),
	}
}

func (cs *ConnectionServer) startAWSProtoListeners() {
	if conf.CFG.SQSServerInterface != "" {
		cs.waitGroup.Add(1)
		go func() {
			defer cs.waitGroup.Done()
			log.Info("SQS service proto", zap.String("interface", conf.CFG.SQSServerInterface))
			mux := http.NewServeMux()
			mux.Handle("/", &sqsproto.SQSRequestHandler{
				ServiceManager: cs.qmgr,
			})
			graceful.Run(conf.CFG.SQSServerInterface, time.Second*10, mux)

		}()
	} else {
		log.Debug("no SQS service enabled")
	}

	if conf.CFG.SNSServerInterface != "" {
		cs.waitGroup.Add(1)
		go func() {
			defer cs.waitGroup.Done()
			log.Info("SNS service proto", zap.String("interface", conf.CFG.SNSServerInterface))
			mux := http.NewServeMux()

			mux.Handle("/", &snsproto.SNSRequestHandler{
				ServiceManager: cs.qmgr,
			})
			graceful.Run(conf.CFG.SNSServerInterface, time.Second*10, mux)
		}()
	} else {
		log.Debug("no SNS service enabled")
	}
}

func (cs *ConnectionServer) startMPQListener() (net.Listener, error) {
	if conf.CFG.FMPQServerInterface != "" {
		log.Info("FireMPQ service", zap.String("interface", conf.CFG.FMPQServerInterface))
		listener, err := net.Listen("tcp", conf.CFG.FMPQServerInterface)
		if err != nil {
			log.Error("could not start FireMPQ listener", zap.Error(err))
			return listener, err
		}
		cs.waitGroup.Add(2)
		go func() {
			<-signals.QuitChan
			listener.Close()
			cs.waitGroup.Done()
		}()
		go func() {
			defer cs.waitGroup.Done()
			defer listener.Close()
			for {
				conn, err := listener.Accept()
				if err != nil {
					select {
					case <-signals.QuitChan:
						return
					default:
						log.Error("Could not accept incoming request", zap.Error(err))
					}
				} else {
					cs.waitGroup.Add(1)
					go cs.handleConnection(conn)
				}
			}
			log.Info("Stopped accepting connections for FireMPQ.")
		}()
		return listener, nil
	} else {
		log.Debug("No FireMPQ Interface configured")
	}
	return nil, nil
}

func (cs *ConnectionServer) Start() {
	signal.Notify(cs.signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	ctx := fctx.Background("shutdown")
	l, err := cs.startMPQListener()
	if err != nil {

		ctx.Error("could not start queue listener", zap.Error(err))
		cs.Shutdown(ctx)
		return
	}
	go cs.waitForSignal(l)
	cs.startAWSProtoListeners()
	cs.waitGroup.Wait()
	cs.Shutdown(ctx)
}

func (cs *ConnectionServer) Shutdown(ctx *fctx.Context) {
	log.Info("Closing queues...")
	cs.qmgr.Close(ctx)
	log.Info("Saving not saved data...")
	db.DatabaseInstance().Close()
	time.Sleep(time.Second)
	log.Info("Server stopped.")
}

func (cs *ConnectionServer) waitForSignal(l net.Listener) {
	<-cs.signalChan
	cs.Stop(fctx.Background("shutdown-signal"))
	signal.Reset(syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	if l != nil {
		l.Close()
	}
}

func (cs *ConnectionServer) Stop(ctx *fctx.Context) {
	ctx.Info("stopping server")
	signals.CloseQuitChan()
}

func (cs *ConnectionServer) handleConnection(conn net.Conn) {
	sh := NewSessionHandler(&cs.waitGroup, conn, cs.qmgr)
	sh.DispatchConn()
	cs.waitGroup.Done()
	conn.Close()
}
