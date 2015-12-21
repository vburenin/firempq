package server

import (
	"firempq/common"
	"firempq/facade"
	"firempq/log"
	"io"
	"net"
	"strings"

	. "firempq/api"
	"strconv"
	"sync"
)

var EOM = []byte{'\n'}

const (
	CMD_PING       = "PING"
	CMD_CREATE_SVC = "CRT"
	CMD_DROP_SVC   = "DROP"
	CMD_QUIT       = "QUIT"
	CMD_UNIX_TS    = "TS"
	CMD_LIST       = "LIST"
	CMD_CTX        = "CTX"
	CMD_LOGLEVEL   = "LOGLEVEL"
	CMD_PANIC      = "PANIC"
	CMD_ASYNC      = "ASYNC"
)

type FuncHandler func([]string) IResponse

type SessionHandler struct {
	connLock  sync.Mutex
	conn      net.Conn
	tokenizer *common.Tokenizer
	active    bool
	ctx       ServiceContext
	svcs      *facade.ServiceFacade
	quitChan  chan struct{}
	asyncChan chan []string
}

func NewSessionHandler(conn net.Conn, services *facade.ServiceFacade) *SessionHandler {

	sh := &SessionHandler{
		conn:      conn,
		tokenizer: common.NewTokenizer(),
		ctx:       nil,
		active:    true,
		svcs:      services,
		asyncChan: make(chan []string, 1024),
	}
	return sh
}

func (s *SessionHandler) QuitListener(quitChan chan struct{}) {
	go func() {
		for {
			select {
			case <-quitChan:
				s.Stop()
				s.conn.Close()
				return
			}
		}
	}()
	go s.asyncDispatcher(quitChan)
}

func logConnError(err error) {
	errTxt := err.Error()
	if err != io.EOF && !(strings.Index(errTxt, "use of closed") > 0) {
		log.Error(errTxt)
	}
}

// DispatchConn dispatcher. Entry point to start connection handling.
func (s *SessionHandler) DispatchConn() {
	addr := s.conn.RemoteAddr().String()
	log.Info("Client connected: %s", addr)
	s.writeResponse(common.NewStrResponse("HELLO FIREMPQ-0.1"))
	for s.active {
		cmdTokens, err := s.tokenizer.ReadTokens(s.conn)
		if err == nil {
			resp := s.processCmdTokens(cmdTokens)
			err = s.writeResponse(resp)
		}
		if err != nil {
			logConnError(err)
			break
		}
	}
	s.conn.Close()
	log.Debug("Client disconnected: %s", addr)

}
func (s *SessionHandler) asyncDispatcher(quitChan chan struct{}) {
	var err error
	for s.active {
		select {
		case <-quitChan:
			return
		case tokens := <-s.asyncChan:
			asyncId := tokens[0]
			resp := common.NewAsyncResponse(asyncId, s.processCmdTokens(tokens[1:]))
			s.connLock.Lock()
			if resp.WriteResponse(s.conn) == nil {
				_, err = s.conn.Write(EOM)
			}
			s.connLock.Unlock()
		}
		if err != nil {
			logConnError(err)
			return
		}
	}
}

// Basic token processing that looks for global commands,
// if there is no token match it will look into current context
// to see if there is a processor for the rest of the tokens.
func (s *SessionHandler) processCmdTokens(cmdTokens []string) IResponse {
	if len(cmdTokens) == 0 {
		return common.OK_RESPONSE
	}

	cmd := cmdTokens[0]
	tokens := cmdTokens[1:]

	switch cmd {
	case CMD_ASYNC:
		return s.asyncHandler(tokens)
	case CMD_QUIT:
		return s.quitHandler(tokens)
	case CMD_CTX:
		return s.ctxHandler(tokens)
	case CMD_CREATE_SVC:
		return s.createServiceHandler(tokens)
	case CMD_DROP_SVC:
		return s.dropServiceHandler(tokens)
	case CMD_LIST:
		return s.listServicesHandler(tokens)
	case CMD_LOGLEVEL:
		return logLevelHandler(tokens)
	case CMD_PING:
		return pingHandler(tokens)
	case CMD_UNIX_TS:
		return tsHandler(tokens)
	case CMD_PANIC:
		return panicHandler(tokens)
	default:
		if s.ctx == nil {
			return common.ERR_UNKNOWN_CMD
		} else {
			return s.ctx.Call(cmd, tokens)
		}
	}
}

func (s *SessionHandler) asyncHandler(tokens []string) IResponse {
	if len(tokens) < 2 {
		return common.InvalidRequest(
			"Asynchrounous request must include at least the request identifier and the command")
	}
	if !common.ValidateItemId(tokens[0]) {
		return common.InvalidRequest("Async call id must be [_a-zA-Z0-9]{1,128}")
	}
	if tokens[1] == CMD_ASYNC {
		return common.InvalidRequest("Recursive async calls are not allowed")
	}
	s.asyncChan <- tokens
	return common.NewStrResponse("A " + tokens[0])
}

// Writes IResponse into connection.
func (s *SessionHandler) writeResponse(resp IResponse) error {
	s.connLock.Lock()
	defer s.connLock.Unlock()
	if err := resp.WriteResponse(s.conn); err != nil {
		return err
	}
	if _, err := s.conn.Write(EOM); err != nil {
		return err
	}
	return nil
}

// Handler that creates a service.
func (s *SessionHandler) createServiceHandler(tokens []string) IResponse {
	if len(tokens) < 2 {
		return common.InvalidRequest("At least service type and name should be provided")
	}
	svcName := tokens[0]
	svcType := tokens[1]

	_, exists := s.svcs.GetService(svcName)
	if exists {
		return common.ConflictRequest("Service exists already")
	}

	resp := s.svcs.CreateService(svcType, svcName, make([]string, 0))
	return resp
}

// Drop service.
func (s *SessionHandler) dropServiceHandler(tokens []string) IResponse {
	if len(tokens) == 0 {
		return common.InvalidRequest("Service name must be provided")
	}
	if len(tokens) > 1 {
		return common.InvalidRequest("DROP accept service name only")
	}
	svcName := tokens[0]
	res := s.svcs.DropService(svcName)
	return res
}

// Context changer.
func (s *SessionHandler) ctxHandler(tokens []string) IResponse {
	if len(tokens) > 1 {
		return common.InvalidRequest("SETCTX accept service name only")
	}

	if len(tokens) == 0 {
		return common.InvalidRequest("Service name must be provided")
	}

	svcName := tokens[0]
	svc, exists := s.svcs.GetService(svcName)
	if !exists {
		return common.ERR_NO_SVC
	}
	s.ctx = svc.NewContext()
	return common.OK_RESPONSE
}

// Stop the processing loop.
func (s *SessionHandler) Stop() {
	s.active = false
}

// Stops the main loop on QUIT.
func (s *SessionHandler) quitHandler(tokens []string) IResponse {
	if len(tokens) > 0 {
		return common.ERR_CMD_WITH_NO_PARAMS
	}
	s.Stop()
	return common.OK_RESPONSE
}

// List all active services.
func (s *SessionHandler) listServicesHandler(tokens []string) IResponse {
	svcPrefix := ""
	svcType := ""
	if len(tokens) == 1 {
		svcPrefix = tokens[0]
	} else if len(tokens) == 2 {
		svcType = tokens[1]
	} else if len(tokens) > 2 {
		return common.InvalidRequest("LIST accept service name prefix and service type only")
	}

	return s.svcs.ListServices(svcPrefix, svcType)
}

// Ping responder.
func pingHandler(tokens []string) IResponse {
	if len(tokens) > 0 {
		return common.ERR_CMD_WITH_NO_PARAMS
	}
	return common.RESP_PONG
}

// Returns current server unix time stamp in milliseconds.
func tsHandler(tokens []string) IResponse {
	if len(tokens) > 0 {
		return common.ERR_CMD_WITH_NO_PARAMS
	}
	return common.NewIntResponse(common.Uts())
}

func logLevelHandler(tokens []string) IResponse {
	if len(tokens) != 1 {
		return common.InvalidRequest("Log level accept one integer parameter in range [0-5]")
	}
	l, e := strconv.Atoi(tokens[0])
	if e != nil || l < 0 || l > 5 {
		return common.InvalidRequest("Log level is an integer in range [0-5]")
	}
	log.Warning("Log level changed to: %d", l)
	log.SetLevel(l)
	return common.OK_RESPONSE
}

func panicHandler(tokens []string) (resp IResponse) {
	if len(tokens) > 0 {
		return common.ERR_CMD_WITH_NO_PARAMS
	}

	log.Critical("Panic requested!")
	panic("Panic requested")
	return common.OK_RESPONSE
}
