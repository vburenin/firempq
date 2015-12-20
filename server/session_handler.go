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
)

type FuncHandler func([]string) IResponse

type SessionHandler struct {
	conn      net.Conn
	tokenizer *common.Tokenizer
	active    bool
	ctx       ServiceContext
	svcs      *facade.ServiceFacade
	quitChan  chan bool
}

func NewSessionHandler(conn net.Conn, services *facade.ServiceFacade) *SessionHandler {

	sh := &SessionHandler{
		conn:      conn,
		tokenizer: common.NewTokenizer(),
		ctx:       nil,
		active:    true,
		svcs:      services,
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
}

// DispatchConn dispatcher. Entry point to start connection handling.
func (s *SessionHandler) DispatchConn() {
	addr := s.conn.RemoteAddr().String()
	log.Info("Client connected: %s", addr)
	s.writeResponse(common.NewStrResponse("HELLO FIREMPQ-0.1"))
	for s.active {
		cmdTokens, err := s.tokenizer.ReadTokens(s.conn)
		if err == nil {
			err = s.processCmdTokens(cmdTokens)
		}
		if err != nil {
			errTxt := err.Error()
			if err != io.EOF && !(strings.Index(errTxt, "use of closed") > 0) {
				log.Error(errTxt)
			}
			break
		}
	}
	s.conn.Close()
	log.Debug("Client disconnected: %s", addr)

}

// Basic token processing that looks for global commands,
// if there is no token match it will look into current context
// to see if there is a processor for the rest of the tokens.
func (s *SessionHandler) processCmdTokens(cmdTokens []string) error {
	var resp IResponse
	if len(cmdTokens) == 0 {
		return s.writeResponse(common.OK_RESPONSE)
	}

	cmd := cmdTokens[0]
	tokens := cmdTokens[1:]

	switch cmd {
	case CMD_QUIT:
		resp = s.quitHandler(tokens)
	case CMD_CTX:
		resp = s.ctxHandler(tokens)
	case CMD_CREATE_SVC:
		resp = s.createServiceHandler(tokens)
	case CMD_DROP_SVC:
		resp = s.dropServiceHandler(tokens)
	case CMD_LIST:
		resp = s.listServicesHandler(tokens)
	case CMD_LOGLEVEL:
		resp = logLevelHandler(tokens)
	case CMD_PING:
		resp = pingHandler(tokens)
	case CMD_UNIX_TS:
		resp = tsHandler(tokens)
	default:
		if s.ctx == nil {
			resp = common.ERR_UNKNOWN_CMD
		} else {
			resp = s.ctx.Call(cmd, tokens)
		}
	}

	return s.writeResponse(resp)
}

// Writes IResponse into connection.
func (s *SessionHandler) writeResponse(resp IResponse) error {
	unsafeData := common.UnsafeStringToBytes(resp.GetResponse())
	if _, err := s.conn.Write(unsafeData); err != nil {
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
