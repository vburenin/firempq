package resp_writer

import (
	"sync"

	"github.com/vburenin/firempq/apis"
)

type TestResponseWriter struct {
	mutex     sync.Mutex
	responses []apis.IResponse
}

func (rw *TestResponseWriter) WriteResponse(resp apis.IResponse) error {
	rw.mutex.Lock()
	rw.responses = append(rw.responses, resp)
	rw.mutex.Unlock()
	return nil
}

func (rw *TestResponseWriter) GetResponses() []apis.IResponse {
	return rw.responses
}

func NewTestResponseWriter() *TestResponseWriter {
	return &TestResponseWriter{
		responses: make([]apis.IResponse, 0, 1000),
	}
}
