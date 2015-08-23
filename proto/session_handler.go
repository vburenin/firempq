package proto

import (
	"bufio"
	"firempq/common"
	"firempq/qerrors"
	"firempq/util"
	"github.com/op/go-logging"
	"io"
	"strings"
	"unicode"
)

var log = logging.MustGetLogger("firempq")

const (
	ENDL          = "\n"
	ENDL_BYTE     = '\n'
	SIMPLE_SERVER = "simple"
)

type FuncHandler func([]string) (common.IResponse, error)

type SessionHandler struct {
	conn         *bufio.ReadWriter
	mainHandlers map[string]FuncHandler
	active       bool
}

func NewSessionHandler(conn *bufio.ReadWriter) *SessionHandler {
	handlerMap := map[string]FuncHandler{
		CMD_PING:    pingHandler,
		CMD_UNIX_TS: tsHandler,
	}
	sh := &SessionHandler{
		conn:         conn,
		mainHandlers: handlerMap,
		active:       true}
	sh.mainHandlers[CMD_QUIT] = sh.quitHandler
	return sh
}

func (s *SessionHandler) DispatchConn() {
	s.writeResponse(common.NewStrResponse("HELLO"))
	for s.active {
		cmdTokens, err := s.readCommand()
		if err != nil {
			if err == io.EOF {
				log.Info("Client disconnected")
				break
			}
			log.Error(err.Error())
		}
		err = s.processCmdTokens(cmdTokens)
	}
	s.conn.Flush()
}

func (s *SessionHandler) readCommand() ([]string, error) {
	data, err := s.conn.ReadString(ENDL_BYTE)

	if err != nil {
		return nil, err
	}

	data = strings.TrimRightFunc(data, unicode.IsSpace)
	splits := strings.Split(data, " ")

	var tokens []string
	for _, s := range splits {
		if len(s) > 0 {
			tokens = append(tokens, s)
		}
	}

	return tokens, nil
}

func (s *SessionHandler) processCmdTokens(cmdTokens []string) error {
	var resp common.IResponse
	var err error
	cmd := cmdTokens[0]
	tokens := cmdTokens[1:]
	handler, ok := s.mainHandlers[cmd]
	if !ok {
		resp = qerrors.ERR_UNKNOW_CMD
	} else {
		resp, err = handler(tokens)
	}
	if err != nil {
		return err
	}
	return s.writeResponse(resp)
}

func (s *SessionHandler) writeResponse(resp common.IResponse) error {

	if _, err := s.conn.WriteString(resp.GetResponse()); err != nil {
		return err
	}
	if _, err := s.conn.WriteString("\n"); err != nil {
		return err
	}
	s.conn.Flush()
	return nil
}

func (s *SessionHandler) quitHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) > 0 {
		return qerrors.ERR_CMD_WITH_NO_PARAMS, nil
	}
	s.active = false
	return common.NewStrResponse("OK"), nil
}

func pingHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) > 0 {
		return qerrors.ERR_CMD_WITH_NO_PARAMS, nil
	}
	return common.NewStrResponse("PONG"), nil
}

func tsHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) > 0 {
		return qerrors.ERR_CMD_WITH_NO_PARAMS, nil
	}
	return common.NewIntResponse(util.Uts()), nil
}
