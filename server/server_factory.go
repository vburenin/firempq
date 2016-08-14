package server

import (
	"errors"

	"github.com/vburenin/firempq/apis"
)

func Server(serverType string, serverAddress string) (apis.IServer, error) {
	if serverType == SimpleServerType {
		return NewServer(), nil
	}
	return nil, errors.New("Invalid server type!")
}
