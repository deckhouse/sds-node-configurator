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
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

type traceIDKey struct{}

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
func WithTraceIDLogger(ctx context.Context, log Logger) Logger {
	traceID := TraceIDFromContext(ctx)
	if traceID == "" {
		return log
	}
	return log.WithValues(traceIDKeyName, traceID)
}

