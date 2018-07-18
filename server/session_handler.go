package server

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/db"
	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/mpqproto"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/signals"
	"github.com/vburenin/firempq/utils"
	"go.uber.org/zap"
)

const (
	CmdPing       = "PING"
	CmdCreateQueu = "CRT"
	CmdDropQueue  = "DROP"
	CmdQuit       = "QUIT"
	CmdUnitTs     = "TS"
	CmdList       = "LIST"
	CmdCtx        = "CTX"
	CmdLogLevel   = "LOGLEVEL"
	CmdPanic      = "PANIC"
	CmdDBStats    = "DBSTATS"
)

type FuncHandler func([]string) apis.IResponse

type SessionHandler struct {
	connLock   sync.Mutex
	conn       net.Conn
	active     bool
	scope      *pqueue.ConnScope
	stopChan   chan struct{}
	tokenizer  *mpqproto.Tokenizer
	qmgr       *pqueue.QueueManager
	connWriter *bufio.Writer
	ctx        *fctx.Context
}

func NewSessionHandler(wg *sync.WaitGroup, conn net.Conn, qmgr *pqueue.QueueManager) *SessionHandler {

	sh := &SessionHandler{
		conn:       conn,
		tokenizer:  mpqproto.NewTokenizer(),
		scope:      nil,
		active:     true,
		qmgr:       qmgr,
		stopChan:   make(chan struct{}),
		connWriter: bufio.NewWriter(conn),
		ctx:        fctx.Background("sess-" + conn.RemoteAddr().String()),
	}
	sh.QuitListener(wg)
	return sh
}

func (s *SessionHandler) QuitListener(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-signals.QuitChan:
			s.Stop()
			s.WriteResponse(mpqerr.ErrConnClosing)
			if s.scope != nil {
				s.scope.Finish()
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
	s.ctx.Debug("new connection", zap.String("addr", addr))
	s.WriteResponse(resp.NewStrResponse("HELLO FIREMPQ-0.1"))
	for s.active {
		cmdTokens, err := s.tokenizer.ReadTokens(s.conn)
		if err != nil {
			if err == io.EOF {
				break
			}
			s.ctx.Warn("connection error", zap.Error(err), zap.String("addr", addr))
			break
		}

		r := s.processCmdTokens(cmdTokens)
		err = s.WriteResponse(r)
		if err != nil {
			s.ctx.Warn("write error", zap.String("addr", addr), zap.Error(err))
			break
		}
	}
	close(s.stopChan)
	if s.scope != nil {
		s.scope.Finish()
	}
	s.conn.Close()
	s.ctx.Debug("client disconnected", zap.String("addr", addr))
}

// Basic token processing that looks for global commands,
// if there is no token match it will look into current context
// to see if there is a processor for the rest of the tokens.
func (s *SessionHandler) processCmdTokens(cmdTokens []string) apis.IResponse {
	if len(cmdTokens) == 0 {
		return resp.OK
	}

	cmd := cmdTokens[0]
	tokens := cmdTokens[1:]

	if s.scope != nil {
		r := s.scope.Call(cmd, tokens)
		if r != nil {
			return r
		}
	}
	switch cmd {
	case CmdQuit:
		return s.quitHandler(tokens)
	case CmdCtx:
		return s.ctxHandler(tokens)
	case CmdCreateQueu:
		return s.createQueueHandler(tokens)
	case CmdDropQueue:
		return s.dropQueueHandler(tokens)
	case CmdList:
		return s.listServicesHandler(tokens)
	case CmdLogLevel:
		return logLevelHandler(tokens)
	case CmdPing:
		return pingHandler(tokens)
	case CmdUnitTs:
		return tsHandler(tokens)
	case CmdPanic:
		return panicHandler(tokens)
	case CmdDBStats:
		return dbstatHandler(tokens)
	default:
		return mpqerr.InvalidRequest("unknown command: " + cmd)
	}
}

// WriteResponse writes apis.IResponse into connection writer.
func (s *SessionHandler) WriteResponse(resp apis.IResponse) error {
	s.connLock.Lock()
	defer s.connLock.Unlock()

	err := resp.WriteResponse(s.connWriter)
	err = s.connWriter.WriteByte('\n')
	err = s.connWriter.Flush()
	return err
}

// Handler that creates a service.
func (s *SessionHandler) createQueueHandler(tokens []string) apis.IResponse {
	if len(tokens) < 1 {
		return mpqerr.InvalidRequest("queue name should be provided")
	}
	if len(tokens) > 1 {
		return mpqerr.InvalidRequest("at least queue name should be provided")
	}

	queueName := tokens[0]
	if len(queueName) > 80 {
		return mpqerr.ErrInvalidQueueName
	}

	if !mpqproto.ValidateItemId(queueName) {
		return mpqerr.ErrInvalidID
	}

	q := s.qmgr.GetQueue(queueName)
	if q != nil {
		return mpqerr.ErrQueueAlreadyExists
	}

	return s.qmgr.CreateQueueFromParams(s.ctx, queueName, tokens[1:])
}

// Drop service.
func (s *SessionHandler) dropQueueHandler(tokens []string) apis.IResponse {
	if len(tokens) == 0 {
		return mpqerr.InvalidRequest("queue name must be provided")
	}
	if len(tokens) > 1 {
		return mpqerr.InvalidRequest("DROP accept queue name only")
	}
	svcName := tokens[0]
	res := s.qmgr.DropQueue(s.ctx, svcName)
	return res
}

// Context changer.
func (s *SessionHandler) ctxHandler(tokens []string) apis.IResponse {
	if len(tokens) > 1 {
		return mpqerr.InvalidRequest("CTX accepts queue name only")
	}

	if len(tokens) == 0 {
		return mpqerr.InvalidRequest("queue name must be provided")
	}

	svcName := tokens[0]
	queue := s.qmgr.GetQueue(svcName)
	if queue == nil {
		return mpqerr.ErrNoQueue
	}
	s.scope = queue.ConnScope(s)
	return resp.OK
}

// Stop the processing loop.
func (s *SessionHandler) Stop() {
	s.active = false
}

// Stops the main loop on QUIT.
func (s *SessionHandler) quitHandler(tokens []string) apis.IResponse {
	if len(tokens) > 0 {
		return mpqerr.ErrCmdNoParamsAllowed
	}
	s.Stop()
	return resp.OK
}

// List all active services.
func (s *SessionHandler) listServicesHandler(tokens []string) apis.IResponse {
	svcPrefix := ""
	if len(tokens) > 1 {
		return mpqerr.InvalidRequest("LIST accepts queue name prefix only")
	}
	if len(tokens) == 1 {
		svcPrefix = tokens[0]
	}

	return resp.NewStrArrayResponse("+SVCLIST", s.qmgr.GetQueueList(svcPrefix))
}

// Ping responder.
func pingHandler(tokens []string) apis.IResponse {
	if len(tokens) > 0 {
		return mpqerr.ErrCmdNoParamsAllowed
	}
	return resp.PONG
}

// Returns current server unix time stamp in milliseconds.
func tsHandler(tokens []string) apis.IResponse {
	if len(tokens) > 0 {
		return mpqerr.ErrCmdNoParamsAllowed
	}
	return resp.NewIntResponse(utils.Uts())
}

func logLevelHandler(tokens []string) apis.IResponse {
	if len(tokens) != 1 {
		return mpqerr.InvalidRequest("Log level accept one integer parameter in range [0-5]")
	}
	l, e := strconv.Atoi(tokens[0])
	if e != nil || l < 0 || l > 5 {
		return mpqerr.InvalidRequest("Log level is an integer in range [0-5]")
	}
	log.Warning("log level change", zap.Int("level", l))
	log.SetLevel(l)
	return resp.OK
}

func panicHandler(tokens []string) apis.IResponse {
	if len(tokens) > 0 {
		return mpqerr.ErrCmdNoParamsAllowed
	}

	log.Critical("Panic requested!")
	panic("Panic requested")
	return resp.OK
}

func dbstatHandler(tokens []string) apis.IResponse {
	if len(tokens) > 0 {
		return mpqerr.ErrCmdNoParamsAllowed
	}
	db := db.DatabaseInstance()
	return resp.NewDictResponse("+DBSTATS", db.GetStats())
}
