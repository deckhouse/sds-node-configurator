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

// Package tracing carries the request trace ID through a context and onto the
// logger.
//
// It used to live in the extender's own logger package. That package is gone --
// the module logs through sds-common-lib/logger now -- but the trace ID is this
// component's own idea and not something a shared logger should know about, so
// it stays here.
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
)

type traceIDKey struct{}

// DryRunHeader marks the response to a dry-run request. The access log reads the mark from
// the response: only the handler decodes the body, and a standby replica that forwards the
// request to the leader gets the leader's response headers back.
const DryRunHeader = "X-Sds-Scheduler-Extender-Dry-Run"

// MarkDryRun marks the response as one to a dry-run request. Call it before the body is written.
func MarkDryRun(w http.ResponseWriter) {
	w.Header().Set(DryRunHeader, "true")
}

// IsDryRun reports whether the response headers carry the dry-run mark.
func IsDryRun(h http.Header) bool {
	return h.Get(DryRunHeader) != ""
}

const traceIDKeyName = "traceid"

// GenerateTraceID generates a unique trace ID.
func GenerateTraceID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// Fallback: use current time in nanoseconds as ID if random generation fails
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// WithTraceID adds a trace ID to the context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

// TraceIDFromContext extracts the trace ID from the context.
// Returns empty string if trace ID is not found.
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	traceID, ok := ctx.Value(traceIDKey{}).(string)
	if !ok {
		return ""
	}
	return traceID
}

// WithTraceIDLogger returns a logger with trace ID from context added as a value.
// If trace ID is not found in context, returns the original logger.
func WithTraceIDLogger(ctx context.Context, log logger.Logger) logger.Logger {
	traceID := TraceIDFromContext(ctx)
	if traceID == "" {
		return log
	}
	return log.With(traceIDKeyName, traceID)
}
