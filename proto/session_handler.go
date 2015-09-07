package proto

import (
	"firempq/common"
	"firempq/facade"
	"github.com/op/go-logging"
	"io"
	"net"
)

var log = logging.MustGetLogger("firempq")

var EOM = []byte{'\n'}

type FuncHandler func([]string) common.IResponse

type SessionHandler struct {
	conn         net.Conn
	tokenizer    *common.Tokenizer
	mainHandlers map[string]FuncHandler
	active       bool
	ctx          common.ISvc
	svcs         *facade.ServiceFacade
}

func NewSessionHandler(conn net.Conn, services *facade.ServiceFacade) *SessionHandler {

	handlerMap := map[string]FuncHandler{
		CMD_PING:    pingHandler,
		CMD_UNIX_TS: tsHandler,
	}
	sh := &SessionHandler{
		conn:         conn,
		tokenizer:    common.NewTokenizer(),
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
	sh.mainHandlers[CMD_CTX] = sh.ctxHandler
	return sh
}

// Connection dispatcher. Entry point to start connection handling.
func (s *SessionHandler) DispatchConn() {
	addr := s.conn.RemoteAddr().String()
	log.Info("Client connected: %s", addr)
	s.writeResponse(common.NewStrResponse("HELLO FIREMPQ-0.1"))
	for s.active {
		cmdTokens, err := s.tokenizer.ReadTokens(s.conn)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Error(err.Error())
		}
		err = s.processCmdTokens(cmdTokens)
	}
	log.Info("Client disconnected: %s", addr)
}

// Basic token processing that looks for global commands,
// if there is no token match it will look into current context
// to see if there is a processor for the rest of the tokens.
func (s *SessionHandler) processCmdTokens(cmdTokens []string) error {
	var resp common.IResponse
	if len(cmdTokens) == 0 {
		return s.writeResponse(common.OK200_RESPONSE)
	}

	cmd := cmdTokens[0]
	tokens := cmdTokens[1:]
	handler, ok := s.mainHandlers[cmd]

	if !ok {
		if s.ctx == nil {
			resp = common.ERR_UNKNOWN_CMD
		} else {
			resp = s.ctx.Call(cmd, tokens)
		}
	} else {
		resp = handler(tokens)
	}

	return s.writeResponse(resp)
}

// Writes IResponse into connection.
func (s *SessionHandler) writeResponse(resp common.IResponse) error {
	unsafeData := common.UnsafeStringToBytes(resp.GetResponse())
	if _, err := s.conn.Write(unsafeData); err != nil {
		return err
	}
	if _, err := s.conn.Write(EOM); err != nil {
		return err
	}
	return nil
}

func (s *SessionHandler) ctxHandler(tokens []string) common.IResponse {
	if len(tokens) < 2 {
		return common.ERR_SVC_CTX
	}
	svcName := tokens[0]
	svc, exists := s.svcs.GetService(svcName)
	if !exists {
		return common.ERR_NO_SVC
	}
	return svc.Call(tokens[1], tokens[2:])
}

// Handler that creates a service.
func (s *SessionHandler) createServiceHandler(tokens []string) common.IResponse {
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
func (s *SessionHandler) dropServiceHandler(tokens []string) common.IResponse {
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
func (s *SessionHandler) setCtxHandler(tokens []string) common.IResponse {
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
	s.ctx = svc
	return common.OK200_RESPONSE
}

// Set stop active flag to false that will case an exit from the processing loop.
func (s *SessionHandler) Stop() {
	s.active = false
}

// Stops the main loop on QUIT.
func (s *SessionHandler) quitHandler(tokens []string) common.IResponse {
	if len(tokens) > 0 {
		return common.ERR_CMD_WITH_NO_PARAMS
	}
	s.Stop()
	return common.OK200_RESPONSE
}

// List all active services.
func (s *SessionHandler) listServicesHandler(tokens []string) common.IResponse {
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
func pingHandler(tokens []string) common.IResponse {
	if len(tokens) > 0 {
		return common.ERR_CMD_WITH_NO_PARAMS
	}
	return common.RESP_PONG
}

// Returns current server unix time stamp in milliseconds.
func tsHandler(tokens []string) common.IResponse {
	if len(tokens) > 0 {
		return common.ERR_CMD_WITH_NO_PARAMS
	}
	return common.NewIntResponse(common.Uts())
}
