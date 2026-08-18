// Package observability provides structured operation logging and gRPC request correlation.
package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"time"
)

type requestIDKey struct{}

// WithRequestID adds a request correlation ID to ctx.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, requestID)
}

// RequestID returns the request correlation ID carried by ctx.
func RequestID(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDKey{}).(string)
	return requestID
}

// EnsureRequestID preserves an existing request ID or creates one.
func EnsureRequestID(ctx context.Context) context.Context {
	if validRequestID(RequestID(ctx)) {
		return ctx
	}
	var id [12]byte
	_, _ = rand.Read(id[:])
	return WithRequestID(ctx, hex.EncodeToString(id[:]))
}

func validRequestID(requestID string) bool {
	if requestID == "" || len(requestID) > 128 {
		return false
	}
	for _, r := range requestID {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '.' {
			continue
		}
		return false
	}
	return true
}

// Logger returns logger enriched with context correlation fields.
func Logger(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if requestID := RequestID(ctx); validRequestID(requestID) {
		return logger.With("request_id", requestID)
	}
	return logger
}

// Start records an info-level operation start and returns its completion recorder.
func Start(ctx context.Context, logger *slog.Logger, component, operation string, attrs ...any) func(error, ...any) {
	return start(ctx, logger, slog.LevelInfo, component, operation, attrs...)
}

// StartDebug records a debug-level operation start and returns its completion recorder. Failures
// are promoted according to error_kind so internal faults remain visible at error level.
func StartDebug(ctx context.Context, logger *slog.Logger, component, operation string, attrs ...any) func(error, ...any) {
	return start(ctx, logger, slog.LevelDebug, component, operation, attrs...)
}

func start(ctx context.Context, logger *slog.Logger, successLevel slog.Level, component, operation string, attrs ...any) func(error, ...any) {
	baseAttrs := make([]any, 0, len(attrs)+4)
	baseAttrs = append(baseAttrs,
		"component", component,
		"operation", operation,
	)
	baseAttrs = append(baseAttrs, attrs...)
	logger = Logger(ctx, logger).With(baseAttrs...)
	started := time.Now()
	logger.Log(ctx, successLevel, "operation started", "phase", "start")

	return func(err error, resultAttrs ...any) {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		attrs := make([]any, 0, len(resultAttrs)+6)
		attrs = append(attrs, resultAttrs...)
		attrs = append(attrs,
			"phase", "finish",
			"outcome", outcome,
			"duration_ms", time.Since(started).Milliseconds(),
		)
		logger.Log(ctx, completionLevel(successLevel, err, resultAttrs), "operation finished", attrs...)
	}
}

func completionLevel(successLevel slog.Level, err error, attrs []any) slog.Level {
	if err == nil {
		return successLevel
	}
	switch stringAttr(attrs, "error_kind") {
	case "invalid_argument", "conflict", "fenced", "canceled", "deadline_exceeded",
		"failed_precondition", "unplaceable", "not_found", "already_exists":
		return slog.LevelInfo
	case "unavailable":
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}

func stringAttr(attrs []any, key string) string {
	for i := 0; i < len(attrs); {
		switch attr := attrs[i].(type) {
		case slog.Attr:
			if attr.Key == key && attr.Value.Kind() == slog.KindString {
				return attr.Value.String()
			}
			i++
		case string:
			if i+1 < len(attrs) && attr == key {
				value, _ := attrs[i+1].(string)
				return value
			}
			i += 2
		default:
			i++
		}
	}
	return ""
}
