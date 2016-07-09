package server

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"time"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/db"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/qmgr"
	"github.com/vburenin/firempq/server/snsproto"
	"github.com/vburenin/firempq/server/sqsproto"
	"github.com/vburenin/firempq/signals"
	"gopkg.in/tylerb/graceful.v1"
)

const (
	SimpleServerType = "simple"
)

type QueueOpFunc func(req []string) error

type ServiceHandler interface {
}

type ConnectionServer struct {
	serviceManager *qmgr.ServiceManager
	listener       net.Listener
	signalChan     chan os.Signal
	waitGroup      sync.WaitGroup
}

func NewServer(listener net.Listener) apis.IServer {
	return &ConnectionServer{
		serviceManager: qmgr.CreateServiceManager(),
		listener:       listener,
		signalChan:     make(chan os.Signal, 1),
	}
}

func (cs *ConnectionServer) startHTTP() {
	go func() {
		if conf.CFG.SQSServerInterface == "" {
			log.Info("No SQS Interface configured")
		}

		log.Info("Starting SQS Server on: %s", conf.CFG.SQSServerInterface)
		cs.waitGroup.Add(1)
		defer cs.waitGroup.Done()

		mux := http.NewServeMux()
		mux.Handle("/", &sqsproto.SQSRequestHandler{
			ServiceManager: cs.serviceManager,
		})
		graceful.Run(conf.CFG.SQSServerInterface, time.Second*10, mux)
	}()

	go func() {
		if conf.CFG.SNSServerInterface == "" {
			log.Info("No SNS Interface configured")
		}
		log.Info("Starting SNS Server on: %s", conf.CFG.SNSServerInterface)

		cs.waitGroup.Add(1)
		defer cs.waitGroup.Done()
		mux := http.NewServeMux()

		mux.Handle("/", &snsproto.SNSRequestHandler{
			ServiceManager: cs.serviceManager,
		})
		graceful.Run(conf.CFG.SNSServerInterface, time.Second*10, mux)
	}()
}

func (cs *ConnectionServer) Start() {
	signal.Notify(cs.signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go cs.waitForSignal()
	cs.startHTTP()

	defer cs.listener.Close()
	for {
		conn, err := cs.listener.Accept()
		if err == nil {
			go cs.handleConnection(conn)
		} else {
			select {
			case <-signals.QuitChan:
				cs.Shutdown()
				return
			default:
				log.Error("Could not accept incoming request: %s", err.Error())
			}
		}
	}
}

func (cs *ConnectionServer) Shutdown() {
	cs.waitGroup.Wait()
	log.Info("Closing queues...")
	cs.serviceManager.Close()
	db.DatabaseInstance().Close()
	log.Info("Server stopped.")
}

func (cs *ConnectionServer) waitForSignal() {
	for {
		select {
		case <-cs.signalChan:
			signal.Reset(syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
			cs.Stop()
			return
		}
	}
}

func (cs *ConnectionServer) Stop() {
	log.Notice("Server has been told to stop.")
	log.Info("Disconnection all clients...")
	cs.listener.Close()
	signals.CloseQuitChan()
}

func (cs *ConnectionServer) handleConnection(conn net.Conn) {
	cs.waitGroup.Add(1)
	session_handler := NewSessionHandler(conn, cs.serviceManager)
	session_handler.DispatchConn()
	cs.waitGroup.Done()
	conn.Close()
}
