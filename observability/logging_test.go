package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestStartRecordsCorrelatedOutcomeWithoutRawError(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	ctx := WithRequestID(context.Background(), "request-1")

	finish := Start(ctx, logger, "test", "work", "session_uid", "session-1")
	finish(errors.New("sensitive error detail"), "error_kind", "expected_failure")

	decoder := json.NewDecoder(&output)
	var started, finished map[string]any
	if err := decoder.Decode(&started); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&finished); err != nil {
		t.Fatal(err)
	}
	if started["request_id"] != "request-1" || started["phase"] != "start" {
		t.Fatalf("unexpected start record: %v", started)
	}
	if finished["outcome"] != "error" || finished["error_kind"] != "expected_failure" {
		t.Fatalf("unexpected finish record: %v", finished)
	}
	if _, ok := finished["error"]; ok {
		t.Fatalf("raw error must not be logged: %v", finished)
	}
}

func TestUnaryInterceptorsPropagateRequestID(t *testing.T) {
	var outgoing metadata.MD
	err := UnaryClientInterceptor(
		WithRequestID(context.Background(), "request-2"),
		"/test.Service/Call",
		nil,
		nil,
		nil,
		func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			outgoing, _ = metadata.FromOutgoingContext(ctx)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	incoming := metadata.NewIncomingContext(context.Background(), outgoing)
	interceptor := UnaryServerInterceptor(slog.New(slog.DiscardHandler))
	_, err = interceptor(
		incoming,
		nil,
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"},
		func(ctx context.Context, _ any) (any, error) {
			if got := RequestID(ctx); got != "request-2" {
				t.Fatalf("request ID = %q, want request-2", got)
			}
			return nil, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnsureRequestIDReplacesUnsafeValue(t *testing.T) {
	unsafe := WithRequestID(context.Background(), "unsafe\nvalue")
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	finish := Start(unsafe, logger, "test", "unsafe_request_id")
	finish(nil)
	var record map[string]any
	if err := json.NewDecoder(&output).Decode(&record); err != nil {
		t.Fatal(err)
	}
	if _, ok := record["request_id"]; ok {
		t.Fatalf("Logger accepted an unsafe request ID: %v", record)
	}

	ctx := EnsureRequestID(unsafe)
	if got := RequestID(ctx); got == "unsafe\nvalue" || !validRequestID(got) {
		t.Fatalf("unsafe request ID was not replaced: %q", got)
	}
}

func TestExpectedFailureUsesInfoLevel(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	finish := Start(context.Background(), logger, "test", "conflict")
	finish(errors.New("conflict"), "error_kind", "conflict")

	decoder := json.NewDecoder(&output)
	var record map[string]any
	if err := decoder.Decode(&record); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&record); err != nil {
		t.Fatal(err)
	}
	if record["level"] != "INFO" || record["outcome"] != "error" {
		t.Fatalf("expected conflict must remain INFO: %v", record)
	}
}

func TestExpectedFailureLevelAcceptsSlogAttr(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	finish := Start(context.Background(), logger, "test", "conflict")
	finish(errors.New("conflict"), slog.String("error_kind", "conflict"))

	decoder := json.NewDecoder(&output)
	var record map[string]any
	if err := decoder.Decode(&record); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&record); err != nil {
		t.Fatal(err)
	}
	if record["level"] != "INFO" {
		t.Fatalf("slog.Attr error_kind did not control level: %v", record)
	}
}

func TestStartDebugSuppressesSuccessfulOperationsAtInfo(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	finish := StartDebug(context.Background(), logger, "test", "internal")
	finish(nil)
	if output.Len() != 0 {
		t.Fatalf("debug operation emitted at INFO: %s", output.String())
	}
}

func TestFinishDoesNotMutateCallerAttrs(t *testing.T) {
	attrs := make([]any, 2, 8)
	attrs[0], attrs[1] = "error_kind", ""
	finish := Start(context.Background(), slog.New(slog.DiscardHandler), "test", "attrs")
	finish(nil, attrs...)
	for i, value := range attrs[:cap(attrs)][len(attrs):] {
		if value != nil {
			t.Fatalf("finish mutated caller attrs backing array at %d: %v", i+len(attrs), value)
		}
	}
}
