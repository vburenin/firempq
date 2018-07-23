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

// NewFireMpqClient makes a first connection to the service to ensure service availability
// and returns a client instance.
func NewFireMpqClient(network, address string) (*FireMpqClient, error) {
	factory := func() (net.Conn, error) {
		return net.Dial(network, address)
	}

	fmc := &FireMpqClient{connFactory: factory}
	t, err := fmc.makeConn()
	if err != nil {
		return nil, err
	}
	t.SendToken(proto.CmdQuit)
	err = t.Complete()
	if err != nil {
		return nil, err
	}
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
		return nil, NewFireMpqError(-3, fmt.Sprintf("Unexpected hello string: %s", connHdr))
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
