package config

import (
	"bytes"
	"encoding/json"
	"firempq/log"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/op/go-logging"
)

type PQueueConfigInfo struct {
	DefaultMessageTtl    int64
	DefaultDeliveryDelay int64
	DefaultLockTimeout   int64
	DefaultPopCountLimit int64
}

type Config struct {
	Port            int
	Interface       string
	DbFlushInterval int64
	DbBufferSize    int64
	LogLevel        int
	PQueueConfig    PQueueConfigInfo
}

var config *Config
var configLock sync.Mutex

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

func GetConfig() *Config {
	if config != nil {
		return config
	}
	configLock.Lock()
	defer configLock.Unlock()
	if config != nil {
		return config
	}
	log.InitLogging(6)
	if config == nil {
		confData, err := ioutil.ReadFile("firempq_cfg.json")

		if err != nil {
			panic(err)
		}

		decoder := json.NewDecoder(bytes.NewReader(confData))
		cfg := &Config{}
		err = decoder.Decode(cfg)
		if err != nil {
			if e, ok := err.(*json.UnmarshalTypeError); ok {
				num, offset, str := getErrorLine(confData, e.Offset)
				log.Error(formatTypeError(num, offset, str, e))
				os.Exit(255)
			}
			log.Error(err.Error())
		}
		config = cfg
		log.InitLogging(logging.Level(config.LogLevel))
	}
	return config
}
