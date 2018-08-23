package server

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"context"

	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/snsproto"
	"github.com/vburenin/firempq/server/sqsproto"
	"github.com/vburenin/firempq/signals"
	"go.uber.org/zap"
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

func waitToStopServer(s *http.Server, timeout time.Duration) {
	<- signals.QuitChan
	log.Error("closing")
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	s.Shutdown(ctx)
}

func (cs *ConnectionServer) startAWSProtoListeners() {
	if conf.CFG.SQSServerInterface != "" {
		cs.waitGroup.Add(1)
		go func() {
			defer cs.waitGroup.Done()
			mux := http.NewServeMux()
			mux.Handle("/", &sqsproto.SQSRequestHandler{
				ServiceManager: cs.qmgr,
			})
			s := &http.Server{Addr: conf.CFG.SQSServerInterface, Handler: mux}
			log.Info("SQS service proto", zap.String("interface", conf.CFG.SQSServerInterface))
			go func() {
				err := s.ListenAndServe()
				if err != nil && err != http.ErrServerClosed{
					log.Error("could not start SQS service", zap.Error(err))
				}
			}()
			waitToStopServer(s, time.Second*10)
		}()
	} else {
		log.Debug("no SQS service enabled")
	}

	if conf.CFG.SNSServerInterface != "" {
		cs.waitGroup.Add(1)
		go func() {
			defer cs.waitGroup.Done()

			mux := http.NewServeMux()

			mux.Handle("/", &snsproto.SNSRequestHandler{
				ServiceManager: cs.qmgr,
			})
			s := &http.Server{Addr: conf.CFG.SNSServerInterface, Handler: mux}
			log.Info("SNS service proto", zap.String("interface", conf.CFG.SNSServerInterface))
			go func() {
				err := s.ListenAndServe()
				if err != nil && err != http.ErrServerClosed {
					log.Error("could not start SNS service", zap.Error(err))
				}
			}()
			waitToStopServer(s, time.Second*10)
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
		cs.waitGroup.Add(1)
		go func() {
			defer cs.waitGroup.Done()
			for {
				conn, err := listener.Accept()
				select {
				case <-signals.QuitChan:
					if conn != nil {
						conn.Close()
					}
					return
				default:
					if err == nil {
						cs.waitGroup.Add(1)
						go cs.handleConnection(conn)
					} else {
						if err != nil {
							log.Error("Could not accept incoming request", zap.Error(err))
						}
					}
				}

			}
			log.Info("stopped accepting connections for FireMPQ.")
		}()
		return listener, nil
	} else {
		log.Debug("no FireMPQ Interface configured")
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
	cs.waitGroup.Add(1)
	go func() {
		<-cs.signalChan
		ctx := fctx.Background("shutdown-signal")
		signal.Reset(syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
		ctx.Info("stopping server")
		signals.CloseQuitChan()
		l.Close()
		cs.waitGroup.Done()
	}()

	cs.startAWSProtoListeners()
	cs.waitGroup.Wait()

	cs.Shutdown(ctx)
}

func (cs *ConnectionServer) Shutdown(ctx *fctx.Context) {
	log.Info("closing queues")
	cs.qmgr.Close(ctx)
	log.Info("server stopped")
}

func (cs *ConnectionServer) handleConnection(conn net.Conn) {
	sh := NewSessionHandler(&cs.waitGroup, conn, cs.qmgr)
	sh.DispatchConn()
	cs.waitGroup.Done()
	conn.Close()
}
