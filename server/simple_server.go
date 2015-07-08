package server


import (
    "bufio"
    "fmt"
    "io"
    "log"
    "net"
    "strings"
    "time"
    "unicode"
    "firempq/proto/text_proto"
    "firempq/queue_facade"
)


func writeResponse(rw *bufio.ReadWriter, line string) error {
    _, err := rw.WriteString(line)
    if err != nil {
        return err
    }
    _, err = rw.WriteString("\n")
    if err != nil {
        return err
    }

    err = rw.Flush()
    if err != nil {
        return err
    }
    return nil
}


func readCommand(rw *bufio.ReadWriter) error {

    data, err := rw.ReadString('\n')

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
            writeResponse(rw, text_proto.RESP_PONG)

        case text_proto.CMD_QUIT:
            writeResponse(rw, text_proto.RESP_BYE)
            return io.EOF

        case text_proto.CMD_UNIX_TS:
            stamp := fmt.Sprint(time.Now().Unix())
            return writeResponse(rw, stamp)

        case text_proto.CMD_CREATE_QUEUE:
            return createQueue(rw, tokens[1:])

        default:
            return writeResponse(rw, text_proto.RESP_ERROR)
    }
    return nil
}


func handleConnection(conn net.Conn) {
    defer conn.Close()

    r := bufio.NewReader(conn)
    w := bufio.NewWriter(conn)

    rw_conn := bufio.NewReadWriter(r, w)

    for {
        err := readCommand(rw_conn)
        if err != nil {
            if err == io.EOF {
                log.Print("Client disconnected")
                break
            }
            log.Print(err.Error())
        }
    }
}


func createQueue(rw *bufio.ReadWriter, req []string) error {
    if len(req) < 1 {
        writeResponse(rw, text_proto.RESP_ERROR + " Not enough parameters")
        return nil
    } else {
        writeResponse(rw, text_proto.RESP_OK)
        return nil
    }
}


type SimpleServer struct {
    address string
    queueFacade *queue_facade.QFacade
}


func NewSimpleServer (address string) IQueueServer {
    return &SimpleServer{address: address,
                         queueFacade: queue_facade.NewPQFacade()}
}


func (srv *SimpleServer) Run()  {

    listener, err := net.Listen("tcp", srv.address)
    if err != nil {
        log.Fatalln("Can't listen to %s: %s", srv.address, err.Error())
    }

    log.Println("Listening at %s", srv.address)
    for {
        conn, err := listener.Accept()
        if err == nil {
            go handleConnection(conn)
        } else {
            log.Println("Could not accept incoming request: %s", err.Error())
        }
    }
}
