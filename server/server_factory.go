package server

import (
	"errors"
	"firempq/common"
	"firempq/log"
	"net"
)

func GetServer(serverClass string, serverAddress string) (common.IServer, error) {
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		return nil, err
	}
	if serverClass == SIMPLE_SERVER {
		log.Info("Listening at %s", serverAddress)
		return NewSimpleServer(listener), nil
	}
	return nil, errors.New("Invalid server class!")
}
