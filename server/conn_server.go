package server

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/db"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/qmgr"
	"github.com/vburenin/firempq/server/sqsproto"
	"github.com/vburenin/firempq/signals"
)

const (
	SimpleServerType = "simple"
)

type QueueOpFunc func(req []string) error

type ConnectionServer struct {
	serviceManager *qmgr.ServiceManager
	listener       net.Listener
	signalChan     chan os.Signal
	waitGroup      sync.WaitGroup
}

func NewSimpleServer(listener net.Listener) apis.IServer {
	return &ConnectionServer{
		serviceManager: qmgr.CreateServiceManager(),
		listener:       listener,
		signalChan:     make(chan os.Signal, 1),
	}
}

func (cs *ConnectionServer) startHTTP() {
	httpServer := http.Server{
		Addr: ":8000",
		Handler: &sqsproto.SQSRequestHandler{
			ServiceManager: cs.serviceManager,
		},
	}
	go httpServer.ListenAndServe()
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
	db.GetDatabase().Close()
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
