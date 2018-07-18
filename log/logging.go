package log

import (
	"io"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.Logger

var Fatal = Logger.Fatal
var Error = Logger.Error
var Critical = Logger.Panic
var Warning = Logger.Warn
var Notice = Logger.Warn
var Info = Logger.Info
var Debug = Logger.Debug

func InitLogging() {
	cfg := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(zapcore.DebugLevel),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey: "message",

			LevelKey:    "level",
			EncodeLevel: zapcore.CapitalLevelEncoder,

			TimeKey:    "time",
			EncodeTime: zapcore.EpochTimeEncoder,

			CallerKey:    "caller",
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}
	Logger, _ = cfg.Build(zap.AddCallerSkip(1))

	Fatal = Logger.Fatal
	Error = Logger.Error
	Critical = Logger.Panic
	Warning = Logger.Warn
	Notice = Logger.Warn
	Info = Logger.Info
	Debug = Logger.Debug
}

func SetLevel(l int) {

}

func LogConnError(err error) {
	errTxt := err.Error()
	if err != io.EOF && !(strings.Index(errTxt, "use of closed") > 0) {
		Error(errTxt)
	}
}
