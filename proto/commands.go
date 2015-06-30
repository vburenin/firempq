package proto

import (
    "net"
    "log"
    "bufio"
    "unicode"
    "strings"
    "io"
)

const (
    CMD_PING = "PING"
    CMD_ASYNC = "ASYNC"
    CMD_PUSH = "PUSH"
    CMD_GET_MSG = "GETM"
    CMD_POP_MSG = "POPM"
    CMD_CREATE_QUEUE = "CRTQ"
    CMD_DROP_QUEUE = "DROPPQ"
    CMD_QUEUE_STATS = "QSTATS"
    CMD_QUIT = "QUIT"
    CMD_UNIX_TS = "TS"
)

var RESP_ERROR = "ERR"
var RESP_PONG = "PONG"
var RESP_OK = "OK"
var RESP_BYE ="BYE"
var RESP_CONTINUE = "CONT"
var RESP_HELLO_WORLD = "HELLO WORLD"

// 384 kb
const MAX_REQ_LENGTH = 393216


var ENDL = []byte{'\n'}

func writeLine(rw *bufio.ReadWriter, line string) error {
    _, err := rw.WriteString(line)
    if err !=nil {
        return err
    }
    _, err = rw.WriteString("\n")
    if err !=nil {
        return err
    }

    err = rw.Flush()
    if err !=nil {
        return err
    }
    return nil
}


func ServeRequests(conn net.Conn) {
    defer conn.Close()

    r := bufio.NewReader(conn)
    w := bufio.NewWriter(conn)

    rw_conn := bufio.NewReadWriter(r, w)

    writeLine(rw_conn, RESP_HELLO_WORLD)

    for {
        err := readCmd(rw_conn)
        if err != nil {
            if err == io.EOF {
                log.Print("Client disconnected")
                break
            }
            log.Print(err.Error())
        }
    }
}

func readCmd(rw *bufio.ReadWriter) (error) {

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

    switch strings.ToUpper(string(splits[0])){
        case CMD_PING:
        writeLine(rw, RESP_PONG)

        case CMD_QUIT:
        writeLine(rw, RESP_BYE)
        return io.EOF

        case "":
        return writeLine(rw, RESP_HELLO_WORLD)

        case CMD_CREATE_QUEUE:
        return createQueue(rw, tokens[1:])

        default:
        return writeLine(rw, RESP_ERROR)
    }
    return nil
}

func createQueue(rw *bufio.ReadWriter, req []string) error {
    if len(req) < 1 {
        writeLine(rw, RESP_ERROR + " Not enough parameters")
        return nil
    } else {
        writeLine(rw, RESP_OK)
        return nil
    }
}