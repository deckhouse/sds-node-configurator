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
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/deckhouse/sds-common-lib/logger"
)

func getFloat64(t *testing.T, m map[string]interface{}, key string) float64 {
	t.Helper()
	// JSON has one number type, so every numeric attribute comes back as a float64
	// regardless of what was passed to the logger.
	f, ok := m[key].(float64)
	if !ok {
		t.Errorf(`f, ok := m[%q].(float64); !ok`, key)
	}
	return f
}

func getString(t *testing.T, m map[string]interface{}, key string) string {
	t.Helper()
	s, ok := m[key].(string)
	if !ok {
		t.Errorf(`s, ok := m[%q].(string); !ok`, key)
	}
	return s
}

func TestAccessLogHandler(t *testing.T) {
	var out bytes.Buffer
	log, err := logger.NewLoggerToWriter(&out, logger.InfoLevel)
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("hello")); err != nil {
			t.Error(err)
		}
	})

	serv := httptest.NewServer(accessLogHandler(log, mux))
	defer serv.Close()

	cli := serv.Client()
	if _, err = cli.Get(serv.URL + "/hello"); err != nil {
		t.Fatal(err)
	}
	if _, err = cli.Get(serv.URL + "/notfound"); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf(`len(accessLogs) != 2, got %d`, len(lines))
	}

	records := make([]map[string]interface{}, len(lines))
	for i, line := range lines {
		if err = json.Unmarshal([]byte(line), &records[i]); err != nil {
			t.Fatalf("record %d is not JSON: %v", i, err)
		}
	}

	helloLog, notfoundLog := records[0], records[1]

	if getFloat64(t, helloLog, "http_status_code") != http.StatusOK {
		t.Error(`getFloat64(t, helloLog, "http_status_code") != http.StatusOK`)
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
	if getFloat64(t, helloLog, "response_size") != 5 {
		t.Error(`getFloat64(t, helloLog, "response_size") != helloLength`)
	}
	if getString(t, helloLog, "traceid") == "" {
		t.Error(`getString(t, helloLog, "traceid") == ""`)
	}
}
