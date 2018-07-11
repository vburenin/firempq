package linear

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"

	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/ferr"
)

const (
	MaxPayloadFileSize = 1024*1024*1024*2 - 1
	MetaFilePrefix     = "metadata"
	PayloadFilePrefix  = "payload"
	DBFileExt          = "ldb"
)

// OpenedFile is an object responsible for adding new data into the blob file and
// retrieving it from the provided position.
// At each position data blob starts with 64bit encoded integer that tells the size of data
// following it.
type OpenedFile struct {
	// Mutex is not used internally, but has to be used externaly in case of the concurrent access to the object.
	sync.Mutex
	curPos   int64
	file     *os.File
	writeBuf *bufio.Writer
	filepath string
	// Work buffer of 8 bytes. Used to avoid memory allocation for every in conversion.
	workBuf8bytes []byte
}

func NewOpenedFile(filepath string, ro bool) (*OpenedFile, error) {
	var f *os.File
	var err error
	var pos int64

	if ro {
		f, err = os.OpenFile(filepath, os.O_RDONLY, 0600)
	} else {
		if data, err := os.Stat(filepath); err == nil {
			if data.IsDir() {
				return nil, ferr.Errorf("file path is a directory: %s", filepath)
			}
			pos = data.Size()
		}
		f, err = os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0600)
		// seek pointer to the end of the file.
		f.Seek(0, 2)
	}

	if err != nil {
		return nil, err
	}

	var wb *bufio.Writer
	if !ro {
		wb = bufio.NewWriterSize(f, 16*1024*1024)
	}
	return &OpenedFile{
		file:          f,
		writeBuf:      wb,
		curPos:        pos,
		workBuf8bytes: make([]byte, 8),
		filepath:      filepath,
	}, nil
}

func (of *OpenedFile) ReopenRO() error {
	if of.writeBuf != nil {
		of.writeBuf.Flush()
	}
	err := of.file.Close()
	if err != nil {
		return err
	}
	of.file, err = os.Open(of.filepath)
	return err
}

func (of *OpenedFile) Flush() error {
	if of.writeBuf != nil {
		return of.writeBuf.Flush()
	}
	return nil
}

func (of *OpenedFile) WriteTo(data []byte) (pos int64, err error) {
	pos = of.curPos

	// write down payload size as 8 bytes.
	enc.Uint64ToBin(uint64(len(data)), of.workBuf8bytes)
	// ignore error here.
	n, err := of.writeBuf.Write(of.workBuf8bytes)
	n, err = of.writeBuf.Write(data)

	of.curPos += int64(n) + 8
	return pos, err
}

func (of *OpenedFile) RetrieveData(pos int64) ([]byte, error) {
	_, err := of.file.ReadAt(of.workBuf8bytes, pos)
	if err != nil {
		return nil, ferr.Wrapf(err, "not enough data to read")
	}

	v := enc.DecodeBytesToUnit64(of.workBuf8bytes)
	if v > MaxPayloadFileSize {
		return nil, ferr.Errorf("too large payload data %d bytes (limit %d bytes)",
			v, MaxPayloadFileSize)
	}

	dataBuff := make([]byte, v)
	_, err = of.file.ReadAt(dataBuff, pos+8)
	if err != nil {
		return nil, ferr.Wrapf(err, "can't load payload body at %d", pos)
	}
	return dataBuff, nil
}

func (of *OpenedFile) Close() error {
	of.Flush()
	return of.file.Close()
}

type FlatStorage struct {
	dbPath string

	mu               sync.Mutex
	curPayloadFileID int64
	payloadFileLimit int64
	curPayloadBlob   *OpenedFile
	payloadCache     *PayloadCache

	// used to sync flush operation with external waiters.
	flushSync chan struct{}

	muMeta        sync.Mutex
	metaDataBuf   *bufio.Writer
	metaDataFile  *os.File
	curMetaFileID int64
	metaPos       int64
	metaFileLimit int64
	metaEncBuf    []byte

	muPayloads     sync.RWMutex
	activePayloads map[int64]*OpenedFile
}

var PayloadFileNotFound = fmt.Errorf("payload file not found")
var NameMatchRE = regexp.MustCompile("^(payload|metadata)-(\\d+)\\.ldb$")

func NewFlatStorage(dbPath string, payloadSizeLimit, metadataSizeLimit int64) (*FlatStorage, error) {
	pathStats, err := os.Stat(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(dbPath, 0755)
			if err != nil {
				return nil, ferr.Wrapf(err, "can't use db path: %s", err)
			}
		} else {
			return nil, ferr.Wrapf(err, "can't use db path: %s", err)
		}
	} else if !pathStats.IsDir() {
		return nil, ferr.Wrapf(err, "db path is not a directory: %s", err)
	}

	files, err := ioutil.ReadDir(dbPath)
	if err != nil {
		return nil, ferr.Wrapf(err, "could not read database content: %s", dbPath)
	}
	metaID, payloadID := findLatestIds(files)

	fs := &FlatStorage{
		dbPath:           dbPath,
		curPayloadFileID: payloadID,
		payloadFileLimit: payloadSizeLimit,
		metaFileLimit:    metadataSizeLimit,
		curMetaFileID:    metaID,
		metaEncBuf:       make([]byte, 8),
		activePayloads:   make(map[int64]*OpenedFile, 64),
		payloadCache:     NewPayloadCache(16384),
		flushSync:        make(chan struct{}),
	}
	if err := fs.metaRollover(); err != nil {
		return nil, err
	}

	if err := fs.payloadRollover(); err != nil {
		return nil, err
	}
	return fs, nil
}

func (fstg *FlatStorage) SyncWait() {
	fstg.mu.Lock()
	c := fstg.flushSync
	fstg.mu.Unlock()
	<-c
}

func (fstg *FlatStorage) flush() error {
	err1 := fstg.metaDataBuf.Flush()
	err2 := fstg.curPayloadBlob.Flush()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	close(fstg.flushSync)
	fstg.flushSync = make(chan struct{})
	return nil
}

func (fstg *FlatStorage) Flush() error {
	fstg.mu.Lock()
	err := fstg.flush()
	fstg.mu.Unlock()
	return err
}

func (fstg *FlatStorage) GetStats() map[string]interface{} {
	return nil
}

func (fstg *FlatStorage) Close() error {
	fstg.mu.Lock()
	defer fstg.mu.Unlock()
	err1 := fstg.metaDataBuf.Flush()
	err2 := fstg.metaDataFile.Close()
	err3 := make([]error, 0)
	for _, f := range fstg.activePayloads {
		if e := f.Close(); e != nil {
			err3 = append(err3, e)
		}
	}
	close(fstg.flushSync)
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err1
	}
	if len(err3) > 0 {
		return err3[0]
	}
	return nil
}

func (fstg *FlatStorage) AddMetadata(metadata []byte) error {
	l := len(metadata)
	fstg.muMeta.Lock()
	fstg.metaDataBuf.WriteByte(byte(l))
	_, err := fstg.metaDataBuf.Write(metadata)
	fstg.metaPos += int64(1 + l)

	if fstg.metaPos > fstg.metaFileLimit {
		err = fstg.metaRollover()
	}

	fstg.muMeta.Unlock()
	return err
}

func (fstg *FlatStorage) openMetaFile() error {
	fn := filepath.Join(fstg.dbPath, MakeMetaFileName(fstg.curMetaFileID))
	pos := int64(0)
	if data, err := os.Stat(fn); err == nil {
		if data.IsDir() {
			return ferr.Wrapf(err, "target file name is a directory: %s", fn)
		}
		pos = data.Size()
	}

	metaDataFile, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return ferr.Wrapf(err, "failed to init metafile db: %s", fn)
	}
	fstg.metaDataFile = metaDataFile
	fstg.metaDataBuf = bufio.NewWriterSize(metaDataFile, 1024*1024)
	fstg.metaPos = pos
	return nil
}

func (fstg *FlatStorage) metaRollover() error {
	if fstg.metaDataFile != nil {
		if err := fstg.metaDataBuf.Flush(); err != nil {
			return ferr.Wrapf(err, "failed to flush metadata buffer")
		}
		if err := fstg.metaDataFile.Close(); err != nil {
			return ferr.Wrapf(err, "failed to close metadata file")
		}
	}

	fstg.curMetaFileID++
	return fstg.openMetaFile()
}

func (fstg *FlatStorage) openPayloadFile(fileID int64, readOnly bool) error {
	fpath := filepath.Join(fstg.dbPath, MakePayloadFileName(fileID))
	payloadFile, err := NewOpenedFile(fpath, readOnly)
	if err != nil {
		if os.IsNotExist(err) {
			return PayloadFileNotFound
		}
		return ferr.Wrapf(err, "could not open payload file %s", fpath)
	}
	fstg.activePayloads[fileID] = payloadFile
	return err
}

func (fstg *FlatStorage) RetrievePayload(fileID, pos int64) ([]byte, error) {
	var payloadFile *OpenedFile
	var err error

	fstg.mu.Lock()
	data := fstg.payloadCache.Payload(fileID, pos)
	fstg.mu.Unlock()
	if data != nil {
		return data, nil
	}

	fstg.muPayloads.Lock()
	payloadFile = fstg.activePayloads[fileID]

	if payloadFile == nil {
		fstg.openPayloadFile(fileID, true)
		payloadFile = fstg.activePayloads[fileID]
		if payloadFile == nil {
			return nil, ferr.Errorf("no payload file with %d id", fileID)
		}
	}
	fstg.muPayloads.Unlock()

	payloadFile.Lock()
	data, err = payloadFile.RetrieveData(pos)
	payloadFile.Unlock()
	return data, err
}

func (fstg *FlatStorage) AddPayload(payload []byte) (int64, int64, error) {
	fstg.mu.Lock()
	fstg.curPayloadBlob.Lock()

	pos, err := fstg.curPayloadBlob.WriteTo(payload)
	fstg.curPayloadBlob.Unlock()

	if err != nil {
		fstg.mu.Unlock()
		return 0, 0, err
	}

	fileID := fstg.curPayloadFileID
	if fstg.payloadCache.AddPayload(fileID, pos, payload) {
		if err := fstg.curPayloadBlob.Flush(); err != nil {
			return 0, 0, ferr.Wrap(err, "failed to flush payload")
		}
	}
	if pos > fstg.payloadFileLimit {
		err = fstg.payloadRollover()
	}

	fstg.mu.Unlock()
	return fileID, pos, err
}

func (fstg *FlatStorage) payloadRollover() error {
	newFileID := fstg.curPayloadFileID + 1
	fpath := filepath.Join(fstg.dbPath, MakePayloadFileName(newFileID))
	newBlob, err := NewOpenedFile(fpath, false)
	if err != nil {
		return ferr.Wrap(err, "failed to rollover to the next payload blob")
	}

	fstg.muPayloads.Lock()
	fstg.activePayloads[newFileID] = newBlob
	fstg.muPayloads.Unlock()

	if fstg.curPayloadBlob != nil {
		fstg.curPayloadBlob.ReopenRO()
	}

	fstg.curPayloadBlob = newBlob
	fstg.curPayloadFileID = newFileID

	return nil
}

func MakePayloadFileName(num int64) string {
	return fmt.Sprintf("%s-%d.%s", PayloadFilePrefix, num, DBFileExt)
}

func MakeMetaFileName(num int64) string {
	return fmt.Sprintf("%s-%d.%s", MetaFilePrefix, num, DBFileExt)
}

func findLatestIds(files []os.FileInfo) (metaID, payloadID int64) {
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		data := NameMatchRE.FindAllStringSubmatch(f.Name(), -1)
		if len(data) > 0 && len(data[0]) == 3 {
			db := data[0][1]
			seq, err := strconv.ParseInt(data[0][2], 10, 64)
			if err != nil {
				continue
			}
			if db == PayloadFilePrefix {
				if seq > payloadID {
					payloadID = seq
				}
			} else if db == MetaFilePrefix {
				if seq > metaID {
					metaID = seq
				}
			}
		}

	}
	return metaID, payloadID
}

func sortMetaFileIds(files []os.FileInfo) []int64 {
	var metafileIDs []int64
	for _, f := range files {
		if f.IsDir() || f.Size() == 0 {
			continue
		}

		data := NameMatchRE.FindAllStringSubmatch(f.Name(), -1)
		if len(data) > 0 && len(data[0]) == 3 {
			db := data[0][1]
			seq, err := strconv.ParseInt(data[0][2], 10, 64)
			if err != nil {
				continue
			}
			if db == MetaFilePrefix {
				metafileIDs = append(metafileIDs, seq)
			}
		}
	}
	sort.Slice(metafileIDs, func(i, j int) bool {
		return metafileIDs[i] < metafileIDs[j]
	})
	return metafileIDs
}
