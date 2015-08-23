package proto

import (
	"bufio"
	"firempq/common"
	"firempq/facade"
	"firempq/svcerr"
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
	ctx          common.ISvc
	svcs         *facade.ServiceFacade
}

func NewSessionHandler(conn *bufio.ReadWriter, services *facade.ServiceFacade) *SessionHandler {
	handlerMap := map[string]FuncHandler{
		CMD_PING:    pingHandler,
		CMD_UNIX_TS: tsHandler,
	}
	sh := &SessionHandler{
		conn:         conn,
		mainHandlers: handlerMap,
		ctx:          nil,
		active:       true,
		svcs:         services,
	}
	sh.mainHandlers[CMD_QUIT] = sh.quitHandler
	sh.mainHandlers[CMD_SETCTX] = sh.setCtxHandler
	return sh
}

func (s *SessionHandler) DispatchConn() {
	s.writeResponse(common.NewStrResponse("HELLO FIREMPQ-0.1"))
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
		resp = svcerr.ERR_UNKNOW_CMD
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

func (s *SessionHandler) setCtxHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) > 1 {
		return svcerr.InvalidRequest("SETCTX accept only service name to be provided"), nil
	}
	if len(tokens) == 0 {
		return svcerr.InvalidRequest("Service name must be provided"), nil
	}
	svcName := tokens[0]
	svc, exists := s.svcs.GetService(svcName)
	if !exists {
		return svcerr.ERR_NO_SVC, nil
	}
	s.ctx = svc
	return common.NewStrResponse("OK"), nil
}

func (s *SessionHandler) quitHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) > 0 {
		return svcerr.ERR_CMD_WITH_NO_PARAMS, nil
	}
	s.active = false
	return common.NewStrResponse("OK"), nil
}

func pingHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) > 0 {
		return svcerr.ERR_CMD_WITH_NO_PARAMS, nil
	}
	return common.NewStrResponse("PONG"), nil
}

func tsHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) > 0 {
		return svcerr.ERR_CMD_WITH_NO_PARAMS, nil
	}
	return common.NewIntResponse(util.Uts()), nil
}
