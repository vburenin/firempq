package server

import (
	"firempq/facade"
	"firempq/iface"
	"firempq/log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const (
	ENDL          = "\n"
	ENDL_BYTE     = '\n'
	SIMPLE_SERVER = "simple"
)

type QueueOpFunc func(req []string) error

type CommandServer struct {
	facade     *facade.ServiceFacade
	listener   net.Listener
	quitChan   chan bool
	signalChan chan os.Signal
	waitGroup  sync.WaitGroup
}

func NewSimpleServer(listener net.Listener) iface.IServer {
	return &CommandServer{
		facade:     facade.CreateFacade(),
		listener:   listener,
		quitChan:   make(chan bool, 1),
		signalChan: make(chan os.Signal, 1),
	}
}

func (this *CommandServer) Start() {
	signal.Notify(this.signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go this.waitForSignal()

	defer this.listener.Close()

	for {
		conn, err := this.listener.Accept()
		if err == nil {
			go this.handleConnection(conn)
		} else {
			select {
			case <-this.quitChan:
				this.Shutdown()
				return
			default:
				log.Error("Could not accept incoming request: %s", err.Error())
			}
		}
	}
}

func (this *CommandServer) Shutdown() {
	this.waitGroup.Wait()
	log.Info("Closing database...")
	this.facade.Close()
	log.Info("Server stopped.")
}

func (this *CommandServer) waitForSignal() {
	for {
		select {
		case <-this.signalChan:
			signal.Reset(syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
			this.Stop()
			return
		}
	}
}

func (this *CommandServer) Stop() {
	log.Notice("Server has been told to stop.")
	log.Info("Disconnection all clients...")
	this.listener.Close()
	close(this.quitChan)
}

func (this *CommandServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	defer this.waitGroup.Done()

	this.waitGroup.Add(1)
	session_handler := NewSessionHandler(conn, this.facade, this.quitChan)
	session_handler.DispatchConn()
}
