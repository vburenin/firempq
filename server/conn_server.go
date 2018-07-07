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
	"gopkg.in/tylerb/graceful.v1"
)

type QueueOpFunc func(req []string) error

type ConnectionServer struct {
	qmgr       *pqueue.QueueManager
	signalChan chan os.Signal
	waitGroup  sync.WaitGroup
}

func NewServer(ctx *fctx.Context) *ConnectionServer {
	return &ConnectionServer{
		qmgr:       pqueue.NewQueueManager(ctx, db.DatabaseInstance(), conf.CFG),
		signalChan: make(chan os.Signal, 1),
	}
}

func (cs *ConnectionServer) startAWSProtoListeners() {
	if conf.CFG.SQSServerInterface != "" {
		cs.waitGroup.Add(1)
		go func() {
			defer cs.waitGroup.Done()
			log.Info("Starting SQS Protocol Server on: %s", conf.CFG.SQSServerInterface)

			mux := http.NewServeMux()
			mux.Handle("/", &sqsproto.SQSRequestHandler{
				ServiceManager: cs.qmgr,
			})
			graceful.Run(conf.CFG.SQSServerInterface, time.Second*10, mux)

		}()
	} else {
		log.Debug("No SQS Interface configured")
	}

	if conf.CFG.SNSServerInterface != "" {
		cs.waitGroup.Add(1)
		go func() {
			defer cs.waitGroup.Done()
			log.Info("Starting SNS Protocol Server on: %s", conf.CFG.SNSServerInterface)

			mux := http.NewServeMux()

			mux.Handle("/", &snsproto.SNSRequestHandler{
				ServiceManager: cs.qmgr,
			})
			graceful.Run(conf.CFG.SNSServerInterface, time.Second*10, mux)
		}()
	} else {
		log.Debug("No SNS interface configured")
	}
}

func (cs *ConnectionServer) startMPQListener() (net.Listener, error) {
	if conf.CFG.FMPQServerInterface != "" {
		log.Info("Starting FireMPQ Protocol Server at %s", conf.CFG.FMPQServerInterface)
		listener, err := net.Listen("tcp", conf.CFG.FMPQServerInterface)
		if err != nil {
			log.Error("Could not start FireMPQ protocol listener: %v", err)
			return listener, err
		}
		cs.waitGroup.Add(1)
		go func() {
			<-signals.QuitChan
			listener.Close()
		}()
		go func() {
			defer cs.waitGroup.Done()
			defer listener.Close()
			for {
				conn, err := listener.Accept()
				if err == nil {
					go cs.handleConnection(conn)
				} else {
					select {
					case <-signals.QuitChan:
						return
					default:
						log.Error("Could not accept incoming request: %v", err)
					}
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
	l, err := cs.startMPQListener()
	if err != nil {
		cs.Shutdown()
		return
	}
	go cs.waitForSignal(l)
	cs.startAWSProtoListeners()
	cs.waitGroup.Wait()
	cs.Shutdown()
}

func (cs *ConnectionServer) Shutdown() {
	log.Info("Closing queues...")
	cs.qmgr.Close()
	log.Info("Saving not saved data...")
	db.DatabaseInstance().Close()
	time.Sleep(time.Second)
	log.Info("Server stopped.")
}

func (cs *ConnectionServer) waitForSignal(l net.Listener) {
	<-cs.signalChan
	signal.Reset(syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	cs.Stop()
	if l != nil {
		l.Close()
	}
}

func (cs *ConnectionServer) Stop() {
	log.Info("Server has been told to stop.")
	log.Info("Disconnection all clients...")
	signals.CloseQuitChan()
}

func (cs *ConnectionServer) handleConnection(conn net.Conn) {
	cs.waitGroup.Add(1)
	sh := NewSessionHandler(conn, cs.qmgr)
	sh.DispatchConn()
	cs.waitGroup.Done()
	conn.Close()
}
