package linear

import (
	"bufio"
	"os"
	"time"

	"sync"

	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/pmsg"
)

const DataFileTimeLength = time.Second * 86400

type PayloadPos struct {
	FileID uint64
	Pos    uint64
}

type LinearDB struct {
	metaWriter *os.File
	writer     *bufio.Writer
	mu         sync.Mutex
}

func OpenDB(dbName string) (*LinearDB, error) {
	f, err := os.Create(dbName + ".firedb")
	if err != nil {
		log.Error("Failed to open database file: %s", err)
		return nil, err
	}
	ldb := &LinearDB{
		metaWriter: f,
		writer:     bufio.NewWriterSize(f, 1024*1024),
	}
	go ldb.flushLoop()
	return ldb, nil
}

func (l *LinearDB) flushLoop() {
	for {
		time.Sleep(time.Second)
		l.mu.Lock()
		if err := l.writer.Flush(); err != nil {
			log.Error("Failed to flush data on disk: %s", err)
		}
		l.mu.Unlock()
	}
}

func (l *LinearDB) writeMeta(m *pmsg.DiskData) error {
	d, _ := m.Marshal()
	l.mu.Lock()
	l.writer.WriteByte(byte(len(d)))
	_, err := l.writer.Write(d)
	l.mu.Unlock()
	if err != nil {
		log.Error("Could not store data: %s", err)
	}
	return err
}

func (l *LinearDB) Add(m *pmsg.PMsgMeta, payload []byte) error {
	return l.writeMeta(&pmsg.DiskData{Meta: m, Action: pmsg.New})
}

func (l *LinearDB) Update(m *pmsg.PMsgMeta) error {
	return l.writeMeta(&pmsg.DiskData{Meta: m, Action: pmsg.Update})
}

func (l *LinearDB) Delete(serial uint64) error {
	return l.writeMeta(&pmsg.DiskData{
		Meta:   &pmsg.PMsgMeta{Serial: serial},
		Action: pmsg.Delete})
}

func (l *LinearDB) Payload(serial uint64) []byte {
	return []byte("data")
}
