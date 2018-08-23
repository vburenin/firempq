package linear

import (
	"bufio"
	"io"
	"os"
	"path/filepath"

	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/ferr"
	"go.uber.org/zap"
)

type MetadataIterator struct {
	metafileIDs []int64
	dbPath      string
	openedFile  *os.File
	reader      *bufio.Reader
	data        []byte
	valid       bool
	ctx         *fctx.Context
	buf         []byte
}

func NewIterator(ctx *fctx.Context, dbPath string) (*MetadataIterator, error) {
	files, err := RetrieveAvailableMetaFileIDs(dbPath)
	if err != nil {
		return nil, err
	}
	return NewMetadataIterator(ctx, dbPath, files)
}

func NewMetadataIterator(ctx *fctx.Context, dbPath string, metaFileIDs []int64) (*MetadataIterator, error) {
	it := &MetadataIterator{
		metafileIDs: metaFileIDs,
		dbPath:      dbPath,
		openedFile:  nil,
		reader:      nil,
		data:        nil,
		valid:       true,
		ctx:         ctx,
		buf:         make([]byte, 65536),
	}
	err := it.nextReader()
	if err == io.EOF {
		return it, nil
	}
	if err != nil {
		return nil, err
	}

	return it, nil
}

func (it *MetadataIterator) Valid() bool {
	return it.valid
}

func (it *MetadataIterator) next() error {
	if it.reader == nil {
		return io.EOF
	}

loop:
	lenByteHigh, _ := it.reader.ReadByte()
	lenByteLow, err := it.reader.ReadByte()
	if err == io.EOF {
		if err := it.nextReader(); err != nil {
			return err
		}
		goto loop
	}
	if err != nil {
		return ferr.Wrapf(err, "could not read element header")
	}
	size := int(lenByteHigh<<8) + int(lenByteLow)

	it.data = it.buf[:size]

	if _, err := io.ReadAtLeast(it.reader, it.data, size); err != nil {
		return ferr.Wrapf(err, "could not read metadata body")
	}
	return nil
}

func (it *MetadataIterator) Next() error {
	e := it.next()
	it.valid = e == nil
	return e
}

func (it *MetadataIterator) GetData() []byte {
	return it.data
}

func (it *MetadataIterator) Close() error {
	if it.openedFile == nil {
		return nil
	}
	return it.openedFile.Close()
}

func (it *MetadataIterator) nextReader() error {
	if len(it.metafileIDs) == 0 {
		return io.EOF
	}

	fn := filepath.Join(it.dbPath, MakeMetaFileName(it.metafileIDs[0]))
	it.ctx.Debug("reading metadata from file", zap.String("filename", fn))

	f, err := os.Open(fn)
	if err != nil {
		return ferr.Wrapf(err, "failed to open database file: %s", fn)
	}

	if it.openedFile != nil {
		if err := it.openedFile.Close(); err != nil {
			return ferr.Wrapf(err, "Failed to close database file %s:", it.openedFile.Name())
		}
	}

	it.openedFile = f
	it.reader = bufio.NewReaderSize(f, 1024*1024)
	it.metafileIDs = it.metafileIDs[1:]
	return nil
}
