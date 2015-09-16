package config

import (
	"encoding/json"
	"log"
	"os"
	"sync"
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
	LogLevel        int64
	PQueueConfig    PQueueConfigInfo
}

var config *Config
var configLock sync.Mutex

func GetConfig() *Config {
	configLock.Lock()
	defer configLock.Unlock()
	if config == nil {
		file, err := os.Open("firempq_cfg.json")
		if err != nil {
			panic(err)
		}
		defer file.Close()
		decoder := json.NewDecoder(file)
		cfg := &Config{}
		err = decoder.Decode(cfg)
		if err != nil {
			err.Error()
			log.Panic(err)
		}
		config = cfg
	}
	return config
}
