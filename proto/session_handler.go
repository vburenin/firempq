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
	sh.mainHandlers[CMD_CREATE_SVC] = sh.createServiceHandler
	sh.mainHandlers[CMD_DROP_SVC] = sh.dropServiceHandler
	sh.mainHandlers[CMD_LIST] = sh.listServicesHandler
	return sh
}

// Connection dispatcher. Entry point to start connection handling.
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

// Command reader and tokenization.
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

// Basic token processing that looks for global commands,
// if there is no token match it will look into current context
// to see if there is a processor for the rest of the tokens.
func (s *SessionHandler) processCmdTokens(cmdTokens []string) error {
	var resp common.IResponse
	var err error
	if len(cmdTokens) == 0 {
		return s.writeResponse(common.RESP_OK)
	}
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

// Writes IResponse into connection.
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

// Handler that creates a service.
func (s *SessionHandler) createServiceHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) < 2 {
		return svcerr.InvalidRequest("At least service type and name should be provided"), nil
	}
	svcName := tokens[0]
	svcType := tokens[1]

	_, exists := s.svcs.GetService(svcName)
	if exists {
		return svcerr.ConflictRequest("Service exists already"), nil
	}

	resp := s.svcs.CreateService(svcType, svcName, make(map[string]string))
	return common.TranslateError(resp), nil
}

// Drop service.
func (s *SessionHandler) dropServiceHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) == 0 {
		return svcerr.InvalidRequest("Service name must be provided"), nil
	}
	if len(tokens) > 1 {
		return svcerr.InvalidRequest("DROP accept service name only"), nil
	}
	svcName := tokens[0]
	res := s.svcs.DropService(svcName)
	return common.TranslateError(res), nil
}

// Context changer.
func (s *SessionHandler) setCtxHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) > 1 {
		return svcerr.InvalidRequest("SETCTX accept service name only"), nil
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
	return common.RESP_OK, nil
}

// Stops the main loop on QUIT.
func (s *SessionHandler) quitHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) > 0 {
		return svcerr.ERR_CMD_WITH_NO_PARAMS, nil
	}
	s.active = false
	return common.RESP_OK, nil
}

// List all active services.
func (s *SessionHandler) listServicesHandler(tokens []string) (common.IResponse, error) {
	svcPrefix := ""
	svcType := ""
	if len(tokens) == 1 {
		svcPrefix = tokens[0]
	} else if len(tokens) == 2 {
		svcType = tokens[1]
	} else if len(tokens) > 2 {
		return svcerr.InvalidRequest("LIST accept service name prefix and service type only"), nil
	}

	list, err := s.svcs.ListServices(svcPrefix, svcType)
	if err != nil {
		return common.TranslateError(err), nil
	}
	resp := common.NewStrArrayResponse(list)
	return resp, nil
}

// Ping responder.
func pingHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) > 0 {
		return svcerr.ERR_CMD_WITH_NO_PARAMS, nil
	}
	return common.RESP_PONG, nil
}

// Returns current server unix time stamp in milliseconds.
func tsHandler(tokens []string) (common.IResponse, error) {
	if len(tokens) > 0 {
		return svcerr.ERR_CMD_WITH_NO_PARAMS, nil
	}
	return common.NewIntResponse(util.Uts()), nil
}
