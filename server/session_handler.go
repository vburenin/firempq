package server

import (
	"firempq/facade"
	"firempq/log"
	"net"

	. "firempq/api"
	. "firempq/common"
	"firempq/db"
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
	CMD_DBSTATS    = "DBSTATS"
)

type FuncHandler func([]string) IResponse

type SessionHandler struct {
	connLock  sync.Mutex
	conn      net.Conn
	tokenizer *Tokenizer
	active    bool
	ctx       ServiceContext
	svcs      *facade.ServiceFacade
	stopChan  chan struct{}
}

func NewSessionHandler(conn net.Conn, services *facade.ServiceFacade) *SessionHandler {
	sh := &SessionHandler{
		conn:      conn,
		tokenizer: NewTokenizer(),
		ctx:       nil,
		active:    true,
		svcs:      services,
		stopChan:  make(chan struct{}),
	}
	sh.QuitListener()
	return sh
}

func (s *SessionHandler) QuitListener() {
	quitChan := GetQuitChan()
	go func() {
		select {
		case <-quitChan:
			s.Stop()
			s.WriteResponse(ERR_CONN_CLOSING)
			if s.ctx != nil {
				s.ctx.Finish()
			}
			s.conn.Close()
			return
		case <-s.stopChan:
		}
	}()
}

// DispatchConn dispatcher. Entry point to start connection handling.
func (s *SessionHandler) DispatchConn() {
	addr := s.conn.RemoteAddr().String()
	log.Debug("Client connected: %s", addr)
	s.WriteResponse(NewStrResponse("HELLO FIREMPQ-0.1"))
	for s.active {
		cmdTokens, err := s.tokenizer.ReadTokens(s.conn)
		if err == nil {
			resp := s.processCmdTokens(cmdTokens)
			err = s.WriteResponse(resp)
		}
		if err != nil {
			log.LogConnError(err)
			break
		}
	}
	close(s.stopChan)
	if s.ctx != nil {
		s.ctx.Finish()
	}
	s.conn.Close()
	log.Debug("Client disconnected: %s", addr)
}

// Basic token processing that looks for global commands,
// if there is no token match it will look into current context
// to see if there is a processor for the rest of the tokens.
func (s *SessionHandler) processCmdTokens(cmdTokens []string) IResponse {
	if len(cmdTokens) == 0 {
		return OK_RESPONSE
	}

	cmd := cmdTokens[0]
	tokens := cmdTokens[1:]

	switch cmd {
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
	case CMD_DBSTATS:
		return dbstatHandler(tokens)
	default:
		if s.ctx == nil {
			return InvalidRequest("Unknown command: " + cmd)
		} else {
			return s.ctx.Call(cmd, tokens)
		}
	}
}

// WriteResponse writes IResponse into connection writer.
func (s *SessionHandler) WriteResponse(resp IResponse) error {
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
		return InvalidRequest("At least service type and name should be provided")
	}
	svcName := tokens[0]
	svcType := tokens[1]

	if len(svcName) > 256 {
		return InvalidRequest("Service name can not be longer than 256 characters")
	}

	if !ValidateItemId(svcName) {
		return ERR_ID_IS_WRONG
	}
	if svcType != STYPE_PRIORITY_QUEUE {
		return InvalidRequest("Unknown service type")
	}

	_, exists := s.svcs.GetService(svcName)
	if exists {
		return ConflictRequest("Service exists already")
	}

	return s.svcs.CreateService(svcType, svcName, tokens[2:])
}

// Drop service.
func (s *SessionHandler) dropServiceHandler(tokens []string) IResponse {
	if len(tokens) == 0 {
		return InvalidRequest("Service name must be provided")
	}
	if len(tokens) > 1 {
		return InvalidRequest("DROP accept service name only")
	}
	svcName := tokens[0]
	res := s.svcs.DropService(svcName)
	return res
}

// Context changer.
func (s *SessionHandler) ctxHandler(tokens []string) IResponse {
	if len(tokens) > 1 {
		return InvalidRequest("CTX accept service name only")
	}

	if len(tokens) == 0 {
		return InvalidRequest("Service name must be provided")
	}

	svcName := tokens[0]
	svc, exists := s.svcs.GetService(svcName)
	if !exists {
		return ERR_NO_SVC
	}
	s.ctx = svc.NewContext(s)
	return OK_RESPONSE
}

// Stop the processing loop.
func (s *SessionHandler) Stop() {
	s.active = false
}

// Stops the main loop on QUIT.
func (s *SessionHandler) quitHandler(tokens []string) IResponse {
	if len(tokens) > 0 {
		return ERR_CMD_WITH_NO_PARAMS
	}
	s.Stop()
	return OK_RESPONSE
}

// List all active services.
func (s *SessionHandler) listServicesHandler(tokens []string) IResponse {
	svcPrefix := ""
	if len(tokens) > 1 {
		return InvalidRequest("LIST accept service name prefix only")
	}
	if len(tokens) == 1 {
		svcPrefix = tokens[0]
	}

	return s.svcs.ListServiceNames(svcPrefix)
}

// Ping responder.
func pingHandler(tokens []string) IResponse {
	if len(tokens) > 0 {
		return ERR_CMD_WITH_NO_PARAMS
	}
	return RESP_PONG
}

// Returns current server unix time stamp in milliseconds.
func tsHandler(tokens []string) IResponse {
	if len(tokens) > 0 {
		return ERR_CMD_WITH_NO_PARAMS
	}
	return NewIntResponse(Uts())
}

func logLevelHandler(tokens []string) IResponse {
	if len(tokens) != 1 {
		return InvalidRequest("Log level accept one integer parameter in range [0-5]")
	}
	l, e := strconv.Atoi(tokens[0])
	if e != nil || l < 0 || l > 5 {
		return InvalidRequest("Log level is an integer in range [0-5]")
	}
	log.Warning("Log level changed to: %d", l)
	log.SetLevel(l)
	return OK_RESPONSE
}

func panicHandler(tokens []string) IResponse {
	if len(tokens) > 0 {
		return ERR_CMD_WITH_NO_PARAMS
	}

	log.Critical("Panic requested!")
	panic("Panic requested")
	return OK_RESPONSE
}

func dbstatHandler(tokens []string) IResponse {
	if len(tokens) > 0 {
		return ERR_CMD_WITH_NO_PARAMS
	}
	db := db.GetDatabase()
	return NewDictResponse("+DBSTATS", db.GetStats())
}
