package testutils

import (
	. "firempq/api"
	"sync"
)

type TestResponseWriter struct {
	mutex     sync.Mutex
	responses []IResponse
}

func (rw *TestResponseWriter) WriteResponse(resp IResponse) error {
	rw.mutex.Lock()
	rw.responses = append(rw.responses, resp)
	rw.mutex.Unlock()
	return nil
}

func (rw *TestResponseWriter) GetResponses() []IResponse {
	return rw.responses
}

func NewTestResponseWriter() *TestResponseWriter {
	return &TestResponseWriter{
		responses: make([]IResponse, 0, 1000),
	}
}
