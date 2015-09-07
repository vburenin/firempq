package server

import (
	"firempq/common"
	"firempq/facade"
	"firempq/proto"
	"log"
	"net"
)

const (
	ENDL          = "\n"
	ENDL_BYTE     = '\n'
	SIMPLE_SERVER = "simple"
)

type QueueOpFunc func(req []string) error

type SimpleServer struct {
	address  string
	facade   *facade.ServiceFacade
	listener net.Listener
	quitChan chan bool
}

func NewSimpleServer(address string) common.IServer {
	return &SimpleServer{address: address,
		facade:   facade.CreateFacade(),
		listener: nil,
		quitChan: make(chan bool)}
}

func (this *SimpleServer) Start() {

	var err error

	this.listener, err = net.Listen("tcp", this.address)
	defer this.listener.Close()

	if err != nil {
		log.Fatalf("Can't listen to %s: %s", this.address, err.Error())
	}

	log.Printf("Listening at %s", this.address)
	for {
		conn, err := this.listener.Accept()
		if err == nil {
			go this.handleConnection(conn)
		} else {
			select {
			case <-this.quitChan:
				log.Printf("Server stopped.")
				return
			default:
				log.Printf("Could not accept incoming request: %s", err.Error())
			}
		}
	}
}

func (this *SimpleServer) Stop() {

	log.Printf("Server has been told to stop.")
	close(this.quitChan)
	this.listener.Close()
}

func (this *SimpleServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	session_handler := proto.NewSessionHandler(conn, this.facade)
	session_handler.DispatchConn()
}
