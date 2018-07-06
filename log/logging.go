package log

import (
	"io"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.SugaredLogger

var Fatal = Logger.Fatalf
var Error = Logger.Errorf
var Critical = Logger.Panicf
var Warning = Logger.Warnf
var Notice = Logger.Warnf
var Info = Logger.Infof
var Debug = Logger.Debugf

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
			EncodeTime: zapcore.ISO8601TimeEncoder,

			CallerKey:    "caller",
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}
	l, _ := cfg.Build(zap.AddCallerSkip(1))

	Logger = l.Sugar()

	Fatal = Logger.Fatalf
	Error = Logger.Errorf
	Critical = Logger.Panicf
	Warning = Logger.Warnf
	Notice = Logger.Warnf
	Info = Logger.Infof
	Debug = Logger.Debugf
}

func SetLevel(l int) {

}

func LogConnError(err error) {
	errTxt := err.Error()
	if err != io.EOF && !(strings.Index(errTxt, "use of closed") > 0) {
		Error(errTxt)
	}
}
