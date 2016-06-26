package server

import (
	"errors"
	"net"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/log"
)

func Server(serverType string, serverAddress string) (apis.IServer, error) {
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		return nil, err
	}
	if serverType == SimpleServerType {
		log.Info("Listening at %s", serverAddress)
		return NewServer(listener), nil
	}
	return nil, errors.New("Invalid server type!")
}
