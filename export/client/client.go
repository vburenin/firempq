package client

import (
	"bufio"
	"fmt"
	"net"

	"github.com/vburenin/firempq/export/proto"
)

type FireMpqClient struct {
	connFactory func() (net.Conn, error)
	version     string
}

const DebugOn = false

// NewFireMpqClient makes a first connection to the service to ensure service availability
// and returns a client instance.
func NewFireMpqClient(network, address string) (*FireMpqClient, error) {
	factory := func() (net.Conn, error) {
		n, err := net.Dial(network, address)
		if err != nil {
			return n, err
		}
		if DebugOn {
			return &NetConnDebug{conn: n}, nil
		}
		return n, nil
	}

	fmc := &FireMpqClient{connFactory: factory}
	return fmc, nil
}

func (fmc *FireMpqClient) GetVersion() string {
	return fmc.version
}

func (fmc *FireMpqClient) makeConn() (*TokenUtil, error) {
	conn, err := fmc.connFactory()
	if err != nil {
		return nil, err
	}
	t := &TokenUtil{
		buf:       bufio.NewWriterSize(conn, 32786),
		conn:      conn,
		tokReader: proto.NewTokenizer(),
	}
	connHdr, err := t.ReadTokens()

	if err != nil {
		return nil, err
	}

	if len(connHdr) == 2 && connHdr[0] == "+HELLO" {
		// TODO(vburenin): Add version check and log warning if version accidentally changes.
		fmc.version = connHdr[1]
	} else {
		t.Close()
		return nil, NewError(-3, fmt.Sprintf("unexpected hello string: %s", connHdr))
	}

	return t, nil
}

func (fmc *FireMpqClient) Queue(queueName string) (*Queue, error) {
	t, err := fmc.makeConn()
	if err != nil {
		return nil, err
	}
	return SetContext(queueName, t)
}

func (fmc *FireMpqClient) CreateQueue(queueName string, opts *QueueParams) (*Queue, error) {
	ns, err := fmc.makeConn()
	if err != nil {
		return nil, err
	}
	return CreateQueue(queueName, ns, opts)
}
