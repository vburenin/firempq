package server

import (
	"firempq/log"
	"firempq/services"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	. "firempq/api"
	"firempq/common"
	"firempq/db"
	"firempq/server/sqsproto"
)

const (
	SIMPLE_SERVER = "simple"
)

type QueueOpFunc func(req []string) error

type ConnectionServer struct {
	serviceManager *services.ServiceManager
	listener       net.Listener
	signalChan     chan os.Signal
	waitGroup      sync.WaitGroup
}

func NewSimpleServer(listener net.Listener) IServer {
	return &ConnectionServer{
		serviceManager: services.CreateServiceManager(),
		listener:       listener,
		signalChan:     make(chan os.Signal, 1),
	}
}

func (self *ConnectionServer) Start() {
	httpServer := http.Server{
		Addr: ":8000",
		Handler: &sqsproto.SQSRequestHandler{
			ServiceManager: self.serviceManager,
		},
	}
	go httpServer.ListenAndServe()

	signal.Notify(self.signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go self.waitForSignal()

	defer self.listener.Close()
	quitChan := common.GetQuitChan()
	for {
		conn, err := self.listener.Accept()
		if err == nil {
			go self.handleConnection(conn)
		} else {
			select {
			case <-quitChan:
				self.Shutdown()
				return
			default:
				log.Error("Could not accept incoming request: %s", err.Error())
			}
		}
	}
}

func (self *ConnectionServer) Shutdown() {
	self.waitGroup.Wait()
	log.Info("Closing queues...")
	self.serviceManager.Close()
	db.GetDatabase().Close()
	log.Info("Server stopped.")
}

func (self *ConnectionServer) waitForSignal() {
	for {
		select {
		case <-self.signalChan:
			signal.Reset(syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
			self.Stop()
			return
		}
	}
}

func (self *ConnectionServer) Stop() {
	log.Notice("Server has been told to stop.")
	log.Info("Disconnection all clients...")
	self.listener.Close()
	common.CloseQuitChan()
}

func (self *ConnectionServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	defer self.waitGroup.Done()

	self.waitGroup.Add(1)
	session_handler := NewSessionHandler(conn, self.serviceManager)
	session_handler.DispatchConn()
}
