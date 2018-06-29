package linear

import (
	"bufio"
	"os"
	"sync"
	"time"

	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/ferr"
)

const (
	MetaFilePrefix    = "metadata"
	PayloadFilePrefix = "payload"
	DBFileExt         = "ldb"
)

type syncWriter struct {
	file *os.File

	mu         sync.Mutex
	writer     *bufio.Writer
	syncPeriod time.Duration
}

func NewSyncWriter(filename string, syncPeriod time.Duration, bufSize int) (*syncWriter, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0664)
	if err != nil {
		return nil, ferr.Wrapf(err, "failed to file: %s", filename)
	}

	sw := &syncWriter{
		file:       f,
		writer:     bufio.NewWriterSize(f, bufSize),
		syncPeriod: syncPeriod,
	}

	if s, err := os.Stat(filename); err == nil {
		if s.IsDir() {
			return nil, ferr.Errorf("database has to be a file: %s", filename)
		}
	}

	ctx := fctx.Background("file sync loop: " + filename)
	sw.startLoop(ctx)
	return sw, nil
}

func (sw *syncWriter) startLoop(ctx *fctx.Context) {
	go func() {
		for {
			sw.mu.Lock()
			s := sw.syncPeriod
			sw.mu.Unlock()

			time.Sleep(s)

			sw.mu.Lock()
			err := sw.writer.Flush()
			sw.mu.Unlock()
			if err != nil {
				ctx.Errorf("Failed to flush data on disk: %s", err)
			}
			sw.mu.Unlock()
		}
	}()
}
