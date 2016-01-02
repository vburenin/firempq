package conf

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/op/go-logging"
)

type PQueueConfigData struct {
	DefaultMessageTtl     int64
	DefaultDeliveryDelay  int64
	DefaultLockTimeout    int64
	DefaultPopCountLimit  int64
	DefaultMaxSize        int64
	TimeoutCheckBatchSize int64

	MaxPopWaitTimeout int64
	MaxPopBatchSize   int64
	MaxLockTimeout    int64
	MaxDeliveryDelay  int64
	MaxMessageTtl     int64
}

// Config is a generic service config type.
type Config struct {
	LogLevel            logging.Level
	Port                int
	Interface           string
	DbFlushInterval     time.Duration
	DbBufferSize        int64
	DatabasePath        string
	PQueueConfig        PQueueConfigData
	UpdateInterval      time.Duration
	BinaryLogPath       string
	BinaryLogBufferSize int
	BinaryLogPageSize   uint64
	BinaryLogFrameSize  uint64
}

var CFG *Config
var CFG_PQ *PQueueConfigData

func init() {
	NewDefaultConfig()
}

func NewDefaultConfig() *Config {
	cfg := Config{
		LogLevel:            logging.INFO,
		Port:                9033,
		Interface:           "",
		DatabasePath:        "./",
		DbFlushInterval:     100,
		DbBufferSize:        10000,
		BinaryLogPath:       "./",
		BinaryLogBufferSize: 128,
		BinaryLogPageSize:   2 * 1024 * 1024 * 1025, // 2Gb
		PQueueConfig: PQueueConfigData{
			// Max number of messages which can be pushed into the queue. 0 - no limit.
			DefaultMaxSize: 0,
			// 10 minutes
			DefaultMessageTtl: 10 * 60 * 1000,
			// No delay
			DefaultDeliveryDelay: 0,
			// Locked by default 60 seconds.
			DefaultLockTimeout: 60 * 1000,
			// Do not time out too many messages at once, it may significantly increase latency.
			TimeoutCheckBatchSize: 1000,
			// Pop wait can not be set larger than 30 seconds.
			MaxPopWaitTimeout: 30000,
			// Max Pop Batch size limit is 10
			MaxPopBatchSize: 10,
			// Max Lock timeout is two hours.
			MaxLockTimeout: 3600000 * 2,
			// Max delivery message delay is 12 hours.
			MaxDeliveryDelay: 3600000 * 12,
			// Max Message TTL is 14 days.
			MaxMessageTtl: 3600000 * 24 * 14,
		},
	}
	CFG = &cfg
	CFG_PQ = &(cfg.PQueueConfig)
	return &cfg
}

func getErrorLine(data []byte, byteOffset int64) (int64, int64, string) {
	var lineNum int64 = 1
	var lineOffset int64
	var lineData []byte
	for idx, b := range data {
		if b < 32 {
			if lineOffset > 0 {
				lineNum++
				lineOffset = 0
				lineData = make([]byte, 0, 32)
			}

		} else {
			lineOffset++
			lineData = append(lineData, b)
		}
		if int64(idx) == byteOffset {
			break
		}
	}
	return lineNum, lineOffset, string(lineData)
}

func formatTypeError(lineNum, lineOffset int64, lineText string, err *json.UnmarshalTypeError) string {
	return fmt.Sprintf(
		"Config error at line %d:%d. Unexpected data type '%s', should be '%s': '%s'",
		lineNum, lineOffset, err.Value, err.Type.String(), strings.TrimSpace(lineText))
}

// ReadConfig reads and decodes firempq_cfg.json file.
func ReadConfig() error {
	confData, err := ioutil.ReadFile("firempq_cfg.json")

	if err != nil {
		return err
	}

	decoder := json.NewDecoder(bytes.NewReader(confData))

	cfg := NewDefaultConfig()
	err = decoder.Decode(cfg)
	if err != nil {
		if e, ok := err.(*json.UnmarshalTypeError); ok {
			num, offset, str := getErrorLine(confData, e.Offset)
			err = errors.New(formatTypeError(num, offset, str, e))
		}
		return err
	}
	CFG = cfg
	CFG_PQ = &(cfg.PQueueConfig)
	return nil
}
