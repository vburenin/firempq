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

type BinaryLogs []*BinLogFile

func (p BinaryLogs) Len() int           { return len(p) }
func (p BinaryLogs) Less(i, j int) bool { return p[i].Num < p[j].Num }
func (p BinaryLogs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type BinaryLog struct {
	dataChan    chan []byte
	serviceChan chan []byte
	seqId       uint64
	maxPageSize uint64
	pageSize    uint64
	frameSize   uint64
	logWriter   io.WriteCloser
	logLocation string
	logFiles    BinaryLogs
}

func NewBinaryLog(cfg conf.Config) *BinaryLog {
	binlog := &BinaryLog{
		dataChan:    make(chan []byte, cfg.BinaryLogBufferSize),
		serviceChan: make(chan []byte, cfg.BinaryLogBufferSize),
		frameSize: cfg.FrameSize,
		seqId:       0,
		maxPageSize: cfg.BinaryLogPageSize,
		pageSize:    0,
		logWriter:   nil,
	}
	logFiles, err := findLogFiles(cfg.BinaryLogPath)
	if err != nil {
		log.Fatal("Coould not init bin log subsystems: %s", err.Error())
	}
	binlog.logFiles = logFiles
	return binlog
}

func (self *BinaryLog) initializeLogFile() *BinLogFile {
	if len(self.logFiles) == 0 {
		firstName := self.logLocation + BinaryLogNamePrefix + "0" + BinaryLogFileExt
		self.logFiles = append(self.logFiles, &BinLogFile{firstName, 0})
	}
	lastLogFile := self.logFiles[len(self.logFiles)-1]

	err := createFileIfNotExists(lastLogFile.FilePath)
	if err != nil {
		log.Fatal("Failed to open a log file: %s", err.Error())
	}
	return lastLogFile
}

func (self *BinaryLog) LogDataChange(exportId uint64, data []byte) {

}

// createFileIfNotExists initializes log file.
func createFileIfNotExists(path string) error {
	_, err := os.Lstat(path)
	if err != nil {
		// What??? It is false if file doesn't exist.
		if os.IsExist(err) {
			log.Fatal("Can not read file stats: %s", err.Error())
		}
		f, err := os.Create(path)
		if err != nil {
			return err
		}

		if err := f.Close(); err != nil {
			return err
		}
		return createFileIfNotExists(path)
	}
	return nil
}

func extractLogFiles(items []os.FileInfo) BinaryLogs {
	candidates := BinaryLogs{}
	for _, item := range items {
		if item.IsDir() {
			continue
		}
		name := item.Name()
		if strings.Index(name, BinaryLogNamePrefix) == 0 {
			chunks := strings.SplitN(name, BinaryLogNamePrefix, 2)
			chunks = strings.SplitN(chunks[1], BinaryLogFileExt, 2)
			num, err := strconv.Atoi(chunks[0])
			if err != nil {
				log.Warning("File matches binary log naming pattern, please remove it: %s", item.Name())
				continue
			}
			candidates = append(candidates, &BinLogFile{name, num})
		}
	}
	sort.Sort(candidates)
	return candidates
}

func findLogFiles(location string) (BinaryLogs, error) {
	items, err := ioutil.ReadDir(location)
	if err != nil {
		log.Critical("Can't read binary log directory: %s", err.Error())
		return nil, err
	}
	return extractLogFiles(items), nil
}
