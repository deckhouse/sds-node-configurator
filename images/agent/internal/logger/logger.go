/*
Copyright 2025 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logger

import (
	"strconv"

	"github.com/go-logr/logr"
	"k8s.io/klog/v2/textlogger"
)

const (
	ErrorLevel   Verbosity = "0"
	WarningLevel Verbosity = "1"
	InfoLevel    Verbosity = "2"
	DebugLevel   Verbosity = "3"
	TraceLevel   Verbosity = "4"
)

const (
	warnLvl = iota + 1
	infoLvl
	debugLvl
	traceLvl
)

type (
	Verbosity string
)

type Logger struct {
	log logr.Logger
}

func NewLogger(level Verbosity) (Logger, error) {
	v, err := strconv.Atoi(string(level))
	if err != nil {
		return Logger{}, err
	}

	log := textlogger.NewLogger(textlogger.NewConfig(textlogger.Verbosity(v))).WithCallDepth(1)

	return Logger{log: log}, nil
}

func NewLoggerWrap(log logr.Logger) Logger {
	return Logger{log: log}
}

// WithName creates a new Logger instance with an additional name component.
// The name is used to identify the source of log messages.
func (l Logger) WithName(name string) Logger {
	return NewLoggerWrap(l.GetLogger().WithName(name))
}

// WithValues creates a new Logger instance with additional key-value pairs.
// These key-value pairs will be included in all subsequent log messages from this logger.
func (l Logger) WithValues(keysAndValues ...any) Logger {
	return NewLoggerWrap(l.GetLogger().WithValues(keysAndValues...))
}

func (l Logger) GetLogger() logr.Logger {
	return l.log
}

func (l Logger) Error(err error, message string, keysAndValues ...interface{}) {
	l.log.WithValues("level", "ERROR").Error(err, message, keysAndValues...)
}

func (l Logger) Warning(message string, keysAndValues ...interface{}) {
	l.log.V(warnLvl).WithValues("level", "WARNING").Info(message, keysAndValues...)
}

func (l Logger) Info(message string, keysAndValues ...interface{}) {
	l.log.V(infoLvl).WithValues("level", "WARNING").Info(message, keysAndValues...)
}

func (l Logger) Debug(message string, keysAndValues ...interface{}) {
	l.log.V(debugLvl).WithValues("level", "DEBUG").Info(message, keysAndValues...)
}

func (l Logger) Trace(message string, keysAndValues ...interface{}) {
	l.log.V(traceLvl).WithValues("level", "TRACE").Info(message, keysAndValues...)
}
