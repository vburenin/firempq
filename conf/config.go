package conf

import (
	"encoding/json"
	"log"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/op/go-logging"
)

type CLIParams struct {
	DefaultMessageTTL    int64
	DefaultDeliveryDelay int64
}

type PQueueConfigData struct {
	DefaultMessageTTL     int64 `long:"msg-ttl" description:"Default message TTL for a new queue in milliseconds" default:"345600000"`
	DefaultDeliveryDelay  int64 `long:"delivery-delay" description:"Default message delivery delay for a new queue in milliseconds" default:"0"`
	DefaultLockTimeout    int64 `long:"lock-timeout" description:"Default message lock/visibility timeout for a new queue in milliseconds" default:"60000"`
	DefaultPopCountLimit  int64 `long:"pop-count-limit" description:"Default receive attempts limit per message for a new queue" default:"99"`
	DefaultMaxQueueSize   int64 `long:"max-queue-size" description:"Default max number of messages per queue" default:"100000000"`
	DefaultPopWaitTimeout int64 `long:"wait-timeout" description:"Default wait timeout to receive a message for a new queue in milliseconds." default:"0"`

	MaxPopWaitTimeout int64 `long:"max-wait-timeout" description:"Limit receive wait timeout. Milliseconds." default:"20000"`
	MaxPopBatchSize   int64 `long:"max-receive-batch" description:"Limit the number of received messages at once." default:"10"`
	MaxLockTimeout    int64 `long:"max-lock-timeout" description:"Max lock/visibility timeout in milliseconds" default:"43200000"`
	MaxDeliveryDelay  int64 `long:"max-delivery-delay" description:"Maximum delivery delay in milliseconds." default:"900000"`
	MaxMessageTTL     int64 `long:"max-message-ttl" description:"Maximum message TTL for the queue. In milliseconds" default:"345600000"`
	MaxMessageSize    int64 `long:"max-message-size" description:"Maximum message size in bytes." default:"262144"`

	TimeoutCheckBatchSize int64 `long:"tune-process-batch" description:"Batch size to process expired messages and message locks. Large number may lead to not desired service timeouts" default:"1000"`
}

// Config is a generic service config type.
type Config struct {
	FMPQServerInterface string `long:"fmpq-address" description:"FireMPQ native protocol." default:":8222"`
	SQSServerInterface  string `long:"sqs-address" description:"SQS protocol interface for FireMPQ" default:""`
	SNSServerInterface  string // `long:"sns-address" description:"NOT IMPLEMENTED: SNS protocol interface for FireMPQ" default:""`
	DbFlushInterval     int64  `long:"flush-interval" description:"Disk synchronization interval in milliseconds" default:"100"`
	DatabasePath        string `long:"data-dir" description:"FireMPQ database location" default:"./fmpq-data"`
	UpdateInterval      int64  `long:"update-interval" description:"Timeout and expiration check period in milliseconds" default:"100"`

	//BinaryLogPath       string
	//BinaryLogBufferSize int
	//BinaryLogPageSize   uint64
	//BinaryLogFrameSize  uint64

	PQueueConfig PQueueConfigData

	TextLogLevel string `long:"log-level" description:"Log level" default:"info" choice:"debug" choice:"info" choice:"warning"`
	PrintConfig  bool   `long:"log-config" description:"Print current config values"`
	Profiler     string `long:"profiler-address" description:"Enables Go profiler on the defined interface:port" default:""`
	LogLevel     logging.Level
}

var CFG *Config
var CFG_PQ *PQueueConfigData

func ParseConfigParameters() *Config {
	cfg := Config{}

	_, err := flags.Parse(&cfg)
	if err != nil {
		os.Exit(255)
	}

	if cfg.TextLogLevel == "warning" {
		cfg.LogLevel = 3
	}

	if cfg.TextLogLevel == "info" {
		cfg.LogLevel = 4
	}

	if cfg.TextLogLevel == "debug" {
		cfg.LogLevel = 5
	}

	if cfg.PrintConfig {
		txt, err := json.MarshalIndent(cfg, " ", "  ")
		if err != nil {
			log.Fatalf("Could not serialize config to print it: %v ", err)
		}
		log.Print(string(txt))
	}

	CFG = &cfg
	CFG_PQ = &(cfg.PQueueConfig)
	return &cfg
}
