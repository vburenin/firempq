package server

import (
	"errors"
	"firempq/common"
)

func GetServer(serverClass string, serverAddress string) (common.IQueueServer, error) {

	if serverClass == SIMPLE_SERVER {
		return NewSimpleServer(serverAddress), nil
	}

	return nil, errors.New("Invalid server class!")
}
