package linear

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

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

type Iterator struct {
	position   int
	files      []string
	reader     *bufio.Reader
	openedFile *os.File
	readBuf    []byte
}

func (it *Iterator) Next() ([]byte, error) {
	if it.openedFile == nil {
		return nil, io.EOF
	}
	sb, err := it.reader.ReadByte()
	size := int(sb)
	if err != nil {
		return nil, err
	}

	buf := it.readBuf[:size]
	n, err := io.ReadAtLeast(it.reader, buf, size)
	if err != nil {
		return nil, err
	}
	log.Info("Expected %d actual %d", size, n)
	return buf, nil
}

func (it *Iterator) Close() error {
	it.position = len(it.files)
	return it.openedFile.Close()
}

func (it *Iterator) nextReader() error {
	if it.position >= len(it.files) {
		return io.EOF
	}
	f, err := os.Open(it.files[it.position])
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		log.Error("Failed to open database file %s: %s", it.files[it.position], err)
		return err
	}

	if it.openedFile != nil {
		if err := it.openedFile.Close(); err != nil {
			log.Error("Failed to close database file %s: %s", it.files[it.position], err)
		}
	}

	it.openedFile = f
	it.reader = bufio.NewReaderSize(f, 1024*1024)
	it.position += 1
	return nil
}

func NewIterator(qid string) (*Iterator, error) {
	it := &Iterator{
		position: 0,
		files:    []string{DBFileName(qid)},
		readBuf:  make([]byte, 255),
	}
	err := it.nextReader()
	return it, err
}

func DBFileName(qid string) string {
	return "fmpq_" + qid + ".firedb"
}

func OpenDB(qid string) (*LinearDB, error) {
	var err error
	var f *os.File
	dbFileName := DBFileName(qid)

	if s, err := os.Stat(dbFileName); err == nil {
		if s.IsDir() {
			log.Error("Failed to open database %s, since it is a directory", dbFileName)
			return nil, fmt.Errorf("Database has to be a file: %s", dbFileName)
		}
	}

	f, err = os.OpenFile(dbFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0664)
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
