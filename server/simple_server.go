package server

import (
	"firempq/common"
	"firempq/facade"
	"firempq/proto"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("firempq")

const (
	ENDL          = "\n"
	ENDL_BYTE     = '\n'
	SIMPLE_SERVER = "simple"
)

type QueueOpFunc func(req []string) error

type SimpleServer struct {
	address    string
	facade     *facade.ServiceFacade
	listener   net.Listener
	quitChan   chan bool
	signalChan chan os.Signal
	waitGroup  sync.WaitGroup
}

func NewSimpleServer(address string) common.IServer {
	return &SimpleServer{address: address,
		facade:     facade.CreateFacade(),
		listener:   nil,
		quitChan:   make(chan bool, 1),
		signalChan: make(chan os.Signal, 1),
	}
}

func (this *SimpleServer) Start() {

	signal.Notify(this.signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go this.waitForSignal()

	var err error

	this.listener, err = net.Listen("tcp", this.address)
	defer this.listener.Close()

	if err != nil {
		log.Fatalf("Can't listen to %s: %s", this.address, err.Error())
	}

	log.Info("Listening at %s", this.address)
	for {
		conn, err := this.listener.Accept()
		if err == nil {
			go this.handleConnection(conn)
		} else {
			select {
			case <-this.quitChan:
				this.waitGroup.Wait()
				log.Info("Closing database...")
				this.facade.Close()
				log.Info("Server stopped.")
				return
			default:
				log.Error("Could not accept incoming request: %s", err.Error())
			}
		}
	}
}

func (this *SimpleServer) waitForSignal() {
	for {
		select {
		case <-this.signalChan:
			signal.Reset(syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
			this.Stop()
			return
		}
	}
}

func (this *SimpleServer) Stop() {
	log.Notice("Server has been told to stop.")
	log.Info("Disconnection all clients...")
	this.listener.Close()
	close(this.quitChan)
}

func (this *SimpleServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	defer this.waitGroup.Done()

	this.waitGroup.Add(1)
	session_handler := proto.NewSessionHandler(conn, this.facade, this.quitChan)
	session_handler.DispatchConn()
}
