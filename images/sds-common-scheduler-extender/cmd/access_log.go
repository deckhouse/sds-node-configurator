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
	"time"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

type accessLogResponseWriter struct {
	http.ResponseWriter
	statusCode int
	size       int
}

func (w *accessLogResponseWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	w.size += n
	return n, err
}

func (w *accessLogResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func accessLogHandler(log logger.Logger, schedulerHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()

		// Generate trace ID for this request
		traceID := logger.GenerateTraceID()
		ctx := logger.WithTraceID(r.Context(), traceID)
		r = r.WithContext(ctx)

		// Create logger with trace ID
		requestLog := logger.WithTraceIDLogger(ctx, log)

		accessLogRW := &accessLogResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		schedulerHandler.ServeHTTP(accessLogRW, r)

		fields := []interface{}{
			"traceid", traceID,
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
		requestLog.Info("[accessLogHandler]", fields...)
	})
}
