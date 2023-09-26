package log

import (
	"flag"
	"github.com/go-logr/logr"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"
)

const (
	ErrorLevel   Verbosity = "0"
	WarningLevel Verbosity = "1"
	InfoLevel    Verbosity = "2"
	DebugLevel   Verbosity = "3"
)

const (
	warnLvl = iota + 1
	infoLvl
	debugLvl
)

type Verbosity string

type KLogger struct {
	log logr.Logger
}

func NewLogger(level Verbosity) (Logger, error) {
	klog.InitFlags(nil)
	if err := flag.Set("v", string(level)); err != nil {
		return nil, err
	}
	flag.Parse()

	log := klogr.New()
	return KLogger{log: log}, nil
}

func (l KLogger) GetLogger() logr.Logger {
	return l.log
}

func (l KLogger) Info(message string, keysAndValues ...interface{}) {
	l.log.V(infoLvl).Info(message, keysAndValues...)
}

func (l KLogger) Warning(message string, keysAndValues ...interface{}) {
	l.log.V(warnLvl).Info(message, keysAndValues...)
}

func (l KLogger) Error(err error, message string, keysAndValues ...interface{}) {
	l.log.Error(err, message, keysAndValues...)
}

func (l KLogger) Debug(message string, keysAndValues ...interface{}) {
	l.log.V(debugLvl).Info(message, keysAndValues...)
}

type Logger interface {
	Info(message string, keysAndValues ...interface{})
	Warning(message string, keysAndValues ...interface{})
	Error(err error, message string, keysAndValues ...interface{})
	Debug(message string, keysAndValues ...interface{})
	GetLogger() klog.Logger
}
