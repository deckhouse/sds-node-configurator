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

package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func getInt64(t *testing.T, m map[string]interface{}, key string) int64 {
	t.Helper()
	i, ok := m[key].(int64)
	if !ok {
		t.Errorf(`i, ok := m[%q].(int64); !ok`, key)
	}
	return i
}
func getString(t *testing.T, m map[string]interface{}, key string) string {
	t.Helper()
	s, ok := m[key].(string)
	if !ok {
		t.Errorf(`s, ok := m[%q].(string); !ok`, key)
	}
	return s
}

// testLoggerWrapper wraps logger.Logger and overrides Info to use direct Info
type testLoggerWrapper struct {
	logger.Logger
	baseLogr logr.Logger
}

// Info overrides the default Info to use direct Info instead of V(2).Info
func (t *testLoggerWrapper) Info(message string, keysAndValues ...interface{}) {
	t.baseLogr.WithValues("level", "INFO").Info(message, keysAndValues...)
}

func TestAccessLogHandler(t *testing.T) {
	obs, logs := observer.New(zap.InfoLevel)
	zapLogger := zap.New(obs)
	logrLogger := zapr.NewLogger(zapLogger)
	baseLogger := logger.NewLoggerWrap(logrLogger)

	// Create a wrapper that overrides Info to use direct Info
	testLogWrapper := &testLoggerWrapper{
		Logger:   baseLogger,
		baseLogr: logrLogger,
	}

	mux := http.NewServeMux()

	// Create a custom handler that uses our test logger
	customHandler := func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		accessLogRW := &accessLogResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		mux.ServeHTTP(accessLogRW, r)

		fields := []interface{}{
			"response_time", time.Since(startTime).Seconds(),
			"protocol", r.Proto,
			"http_status_code", accessLogRW.statusCode,
			"http_method", r.Method,
			"url", r.RequestURI,
			"http_host", r.Host,
			"request_size", r.ContentLength,
			"response_size", accessLogRW.size,
		}
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err == nil {
			fields = append(fields, "remote_ipaddr", ip)
		}
		ua := r.Header.Get("User-Agent")
		if len(ua) > 0 {
			fields = append(fields, "http_user_agent", ua)
		}
		testLogWrapper.Info("[accessLogHandler] access", fields...)
	}
	mux.HandleFunc("/hello", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		// w.Write([]byte("hello"))
		_, err := w.Write([]byte("hello"))
		if err != nil {
			t.Fatal(err)
		}
	})
	serv := httptest.NewServer(http.HandlerFunc(customHandler))
	defer serv.Close()

	cli := serv.Client()
	_, err := cli.Get(serv.URL + "/hello")
	if err != nil {
		t.Fatal(err)
	}
	_, err = cli.Get(serv.URL + "/notfound")
	if err != nil {
		t.Fatal(err)
	}

	if logs.Len() != 2 {
		t.Fatal(`len(accessLogs) != 2`)
	}

	helloLog := logs.All()[0].ContextMap()
	notfoundLog := logs.All()[1].ContextMap()

	if getInt64(t, helloLog, "http_status_code") != http.StatusOK {
		t.Error(`getInt(t, helloLog, "http_status_code") != http.StatusOK`)
	}
	if getString(t, helloLog, "http_method") != "GET" {
		t.Error(`getString(t, helloLog, "http_method") != "GET"`)
	}
	if getString(t, helloLog, "url") != "/hello" {
		t.Error(`getString(t, helloLog, "url") != "/hello"`)
	}
	if getString(t, notfoundLog, "url") != "/notfound" {
		t.Error(`getString(t, notfoundLog, "url") != "/notfound"`)
	}
	if getInt64(t, helloLog, "response_size") != 5 {
		t.Error(`getInt(t, helloLog, "response_size") != helloLength`)
	}
}
