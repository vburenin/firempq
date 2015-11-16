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
	DefaultMessageTtl    int64
	DefaultDeliveryDelay int64
	DefaultLockTimeout   int64
	DefaultPopCountLimit int64
	ExpirationBatchSize  int64
	UnlockBatchSize      int64
	MaxPopWaitTimeout    int64
	MaxPopBatchSize      int64
}

type DSQueueConfigData struct {
	DefaultMessageTtl    int64
	DefaultDeliveryDelay int64
	DefaultLockTimeout   int64
	DefaultPopCountLimit int64
	ExpirationBatchSize  int64
	UnlockBatchSize      int64
	MaxPopWaitTimeout    int64
	MaxPopBatchSize      int64
}

type Config struct {
	Port                int
	Interface           string
	DbFlushInterval     time.Duration
	DbBufferSize        int64
	LogLevel            logging.Level
	PQueueConfig        PQueueConfigData
	DSQueueConfig       DSQueueConfigData
	UpdateInterval      time.Duration
	BinaryLogPath       string
	BinaryLogBufferSize int
	BinaryLogPageSize   uint64
	BinaryLogFrameSize  uint64
}

func NewDefaultConfig() *Config {
	cfg := Config{
		Port:                9033,
		Interface:           "",
		DbFlushInterval:     100,
		DbBufferSize:        10000,
		LogLevel:            logging.INFO,
		BinaryLogPath:       "./",
		BinaryLogBufferSize: 128,
		BinaryLogPageSize:   2 * 1024 * 1024 * 1025, // 2Gb
		PQueueConfig: PQueueConfigData{
			DefaultMessageTtl:    10 * 60 * 1000,
			DefaultDeliveryDelay: 0,
			DefaultLockTimeout:   60 * 1000,
			DefaultPopCountLimit: 0,
			ExpirationBatchSize:  1000,
			UnlockBatchSize:      1000,
			MaxPopWaitTimeout:    30000,
			MaxPopBatchSize:      10,
		},
		DSQueueConfig: DSQueueConfigData{
			DefaultMessageTtl:    10 * 60 * 1000,
			DefaultDeliveryDelay: 0,
			DefaultLockTimeout:   60 * 1000,
			DefaultPopCountLimit: 0,
			ExpirationBatchSize:  1000,
			UnlockBatchSize:      1000,
			MaxPopWaitTimeout:    30000,
			MaxPopBatchSize:      10,
		},
	}
	return &cfg
}

var CFG *Config = NewDefaultConfig()

func getErrorLine(data []byte, byteOffset int64) (int64, int64, string) {
	var lineNum int64 = 1
	var lineOffset int64 = 0
	var lineData []byte
	for idx, b := range data {
		if b < 32 {
			if lineOffset > 0 {
				lineNum += 1
				lineOffset = 0
				lineData = make([]byte, 0, 32)
			}

		} else {
			lineOffset += 1
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
	return nil
}
