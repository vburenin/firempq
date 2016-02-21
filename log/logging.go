package log

import (
	"firempq/conf"
	"io"
	"log"
	"os"
	"strings"

	"github.com/op/go-logging"
)

func InitLogging() {
	format := logging.MustStringFormatter(
		"%{color}%{time:2006-01-02 15:04:05.00000}: %{level}%{color:reset} %{shortfile} %{message}",
	)
	logbackend := logging.NewLogBackend(os.Stderr, "", 0)
	formatter := logging.NewBackendFormatter(logbackend, format)
	logging.SetBackend(formatter)
	logging.SetLevel(conf.CFG.LogLevel, "firempq")
	fixLogger()
}

func fixLogger() {
	Logger.ExtraCalldepth = 1
}

func SetLevel(l int) {
	logging.SetLevel(logging.Level(l), "firempq")
}

var Logger = logging.MustGetLogger("firempq")

var Error = Logger.Errorf
var Critical = Logger.Criticalf
var Warning = Logger.Warningf
var Notice = Logger.Noticef
var Info = Logger.Infof
var Debug = Logger.Debugf
var Fatal = log.Fatalf

func LogConnError(err error) {
	errTxt := err.Error()
	if err != io.EOF && !(strings.Index(errTxt, "use of closed") > 0) {
		Error(errTxt)
	}
}
