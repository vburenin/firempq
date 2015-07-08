package server

import (
	"errors"
)

type IQueueServer interface {
	Run()
}

func ServerFactory(serverClass string, serverAddress string) (IQueueServer, error) {
	if serverClass == "simple" {
		return NewSimpleServer(serverAddress), nil
	}

	return nil, errors.New("Invalid server class!")
}
