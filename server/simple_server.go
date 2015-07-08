package server

import (
	"bufio"
	"firempq/factory/queue_factory"
	"firempq/proto/text_proto"
	"firempq/queue_facade"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"
	"unicode"
)

const (
	ENDL          = "\n"
    ENDL_BYTE     = '\n'
	SIMPLE_SERVER = "simple"
)

type SimpleServer struct {
	address     string
	queueFacade *queue_facade.QFacade
}

func NewSimpleServer(address string) IQueueServer {
	return &SimpleServer{address: address,
		queueFacade: queue_facade.NewPQFacade()}
}

func (this *SimpleServer) Run() {

	listener, err := net.Listen("tcp", this.address)
	if err != nil {
		log.Fatalln("Can't listen to %s: %s", this.address, err.Error())
	}

	log.Println("Listening at %s", this.address)
	for {
		conn, err := listener.Accept()
		if err == nil {
			go this.handleConnection(conn)
		} else {
			log.Println("Could not accept incoming request: %s", err.Error())
		}
	}
}

func (this *SimpleServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	rw_conn := bufio.NewReadWriter(r, w)

	for {
		err := this.readCommand(rw_conn)
		if err != nil {
			if err == io.EOF {
				log.Print("Client disconnected")
				break
			}
			log.Print(err.Error())
		}
	}
}

func (this *SimpleServer) readCommand(rw *bufio.ReadWriter) error {

	data, err := rw.ReadString(ENDL_BYTE)

	if err != nil {
		return err
	}

	data = strings.TrimRightFunc(data, unicode.IsSpace)
	splits := strings.Split(data, " ")
	var tokens []string
	for _, s := range splits {
		if len(s) > 0 {
			tokens = append(tokens, s)
		}
	}

	if len(splits) == 0 {
		return nil
	}

	switch strings.ToUpper(string(splits[0])) {
	case text_proto.CMD_PING:
		this.writeResponse(rw, text_proto.RESP_PONG)

	case text_proto.CMD_QUIT:
		this.writeResponse(rw, text_proto.RESP_BYE)
		return io.EOF

	case text_proto.CMD_UNIX_TS:
		stamp := fmt.Sprint(time.Now().Unix())
		return this.writeResponse(rw, stamp)

	case text_proto.CMD_CREATE_QUEUE:
		return this.createQueue(rw, tokens[1:])

	case text_proto.CMD_DROP_QUEUE:
		return this.dropQueue(rw, tokens[1:])

	default:
		return this.writeResponse(rw, text_proto.RESP_ERROR)
	}
	return nil
}

func (this *SimpleServer) writeResponse(rw *bufio.ReadWriter, line string) error {
	_, err := rw.WriteString(line)
	if err != nil {
		return err
	}
	_, err = rw.WriteString(ENDL)
	if err != nil {
		return err
	}

	err = rw.Flush()
	if err != nil {
		return err
	}
	return nil
}

type QueueFunc func(req []string) error

func (this *SimpleServer) queueOp(rw *bufio.ReadWriter, args []string, queueFunc QueueFunc) error {
	if len(args) < 1 {
		return this.writeResponse(rw, text_proto.RESP_ERROR+" Not enough parameters")
	}

	if err := queueFunc(args); err == nil {
		return this.writeResponse(rw, text_proto.RESP_OK)
	} else {
		return this.writeResponse(rw, text_proto.RESP_ERROR+" "+err.Error())
	}
}

func (this *SimpleServer) createQueue(rw *bufio.ReadWriter, req []string) error {
	return this.queueOp(rw, req,
		func(args []string) error {
			return this.queueFacade.AddQueue(args[0], queue_factory.GetPQueue(args[0]))
		})
}

func (this *SimpleServer) dropQueue(rw *bufio.ReadWriter, req []string) error {
	return this.queueOp(rw, req,
		func(args []string) error {
			return this.queueFacade.DropQueue(args[0])
		})
}
