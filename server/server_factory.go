package server

import (
	"errors"
	"firempq/log"
	"net"

	. "firempq/api"
)

func Server(serverType string, serverAddress string) (IServer, error) {
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		return nil, err
	}
	if serverType == SimpleServerType {
		log.Info("Listening at %s", serverAddress)
		return NewSimpleServer(listener), nil
	}
	return nil, errors.New("Invalid server type!")
}
