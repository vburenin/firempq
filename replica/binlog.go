package replica

import (
	"firempq/conf"
	"firempq/log"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	BinaryLogFileName     = "ServiceBinaryLog"
	BinaryLogFileExt      = ".bin"
	BinaryLogNumSeparator = "_"
	BinaryLogNamePrefix   = BinaryLogFileName + BinaryLogNumSeparator
)

type BinLogFile struct {
	FilePath string
	Num      int
}

func (self *BinLogFile) String() string {
	return fmt.Sprintf("FileName: %s, Position: %d", self.FilePath, self.Num)
}

type BinLogs []*BinLogFile

func (p BinLogs) Len() int           { return len(p) }
func (p BinLogs) Less(i, j int) bool { return p[i].Num < p[j].Num }
func (p BinLogs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type BinaryLog struct {
	dataChan    chan []byte
	serviceChan chan []byte
	seqId       uint64
	maxPageSize uint64
	pageSize    uint64
	logWriter   io.WriteCloser
	logReader   io.ReadCloser
	logLocation string
	logFiles    BinLogs
}

func NewBinaryLog(cfg conf.Config) *BinaryLog {
	binlog := &BinaryLog{
		dataChan:    make(chan []byte, cfg.BinaryLogBufferSize),
		serviceChan: make(chan []byte, cfg.BinaryLogBufferSize),
		seqId:       0,
		maxPageSize: cfg.BinaryLogPageSize,
		pageSize:    0,
		logWriter:   nil,
		logReader:   nil,
	}
	return binlog
}

func extractLogFiles(items []os.FileInfo) BinLogs {
	candidates := BinLogs{}
	for _, item := range items {
		if item.IsDir() {
			continue
		}
		name := item.Name()
		if strings.Index(name, BinaryLogNamePrefix) == 0 {
			chunks := strings.SplitN(name, BinaryLogNamePrefix, 2)
			chunks = strings.SplitN(chunks[1], BinaryLogFileExt, 2)
			num, cnverr := strconv.Atoi(chunks[0])
			if cnverr != nil {
				log.Warning("File matches binary log naming pattern, please remove it: %s", item.Name())
				continue
			}
			candidates = append(candidates, &BinLogFile{name, num})
		}
	}
	sort.Sort(candidates)
	return candidates
}

func findLogFiles(location string) (BinLogs, error) {
	items, err := ioutil.ReadDir(location)
	if err != nil {
		log.Critical("Can't read binary log directory: %s", err.Error())
		return nil, err
	}
	return extractLogFiles(items), nil
}
