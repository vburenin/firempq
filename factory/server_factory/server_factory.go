package server_factory

import (
	"errors"
	"firempq/server"
)

func GetServer(serverClass string, serverAddress string) (server.IQueueServer, error) {

	if serverClass == server.SIMPLE_SERVER {
		return server.NewSimpleServer(serverAddress), nil
	}

	return nil, errors.New("Invalid server class!")
}
