package wire_test

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/wire"
)

func TestNormalizeToolArgs(t *testing.T) {
	for _, test := range []struct {
		name string
		args map[string]any
		want map[string]any
	}{
		{"nil", nil, nil},
		{"empty object", map[string]any{}, map[string]any{}},
		{"numeric types", map[string]any{"int": int(2), "signed": int64(-2), "unsigned": uint64(2), "float": 2.0, "number": json.Number("2.0")}, map[string]any{"int": float64(2), "signed": float64(-2), "unsigned": float64(2), "float": float64(2), "number": float64(2)}},
		{"nested", map[string]any{"b": true, "s": "fixture", "null": nil, "nested": map[string]any{"items": []any{1, "a", nil}}}, map[string]any{"b": true, "s": "fixture", "null": nil, "nested": map[string]any{"items": []any{float64(1), "a", nil}}}},
		{"typed containers", map[string]any{"items": []string{"a", "b"}, "values": map[string]int{"x": 2}}, map[string]any{"items": []any{"a", "b"}, "values": map[string]any{"x": float64(2)}}},
		{"numeric boundary", map[string]any{"max": int64(9007199254740991), "min": int64(-9007199254740991)}, map[string]any{"max": float64(9007199254740991), "min": float64(-9007199254740991)}},
		{"nil native containers preserve Struct semantics", map[string]any{"map": map[string]any(nil), "list": []any(nil)}, map[string]any{"map": map[string]any{}, "list": []any{}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := wire.NormalizeToolArgs(test.args)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("normalization: got %#v want %#v", got, test.want)
			}
		})
	}
}

type namedToolNumber int

func TestNormalizeToolArgsPreservesExistingFallbackRepresentation(t *testing.T) {
	for _, test := range []struct {
		name       string
		args, want map[string]any
	}{
		{"native", map[string]any{"count": 2, "optional": map[string]any(nil)}, map[string]any{"count": float64(2), "optional": map[string]any{}}},
		{"named number fallback", map[string]any{"count": namedToolNumber(2), "optional": map[string]any(nil)}, map[string]any{"count": float64(2), "optional": nil}},
	} {
		t.Run(test.name, func(t *testing.T) {
			legacy := wire.ToolCallFromProto(wire.ToolCallToProto(&api.ToolCall{Args: test.args})).Args
			if !reflect.DeepEqual(legacy, test.want) {
				t.Fatalf("legacy conversion: got %#v want %#v", legacy, test.want)
			}
			got, err := wire.NormalizeToolArgs(test.args)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("changed legacy representation: got %#v want %#v", got, test.want)
			}
		})
	}
}

type marshaledToolObject map[string]any

func (marshaledToolObject) MarshalJSON() ([]byte, error) { return []byte(`{}`), nil }

type marshaledToolText string

func (marshaledToolText) MarshalText() ([]byte, error) { return []byte("transformed"), nil }

func TestNormalizeToolArgsRejectsNonJSONValues(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle
	for _, test := range []struct {
		name  string
		value any
	}{
		{"function", func() {}},
		{"custom marshaler", marshaledToolObject{"hidden": "value"}},
		{"custom text marshaler", marshaledToolText("value")},
		{"custom object key", map[marshaledToolText]any{"value": nil}},
		{"channel", make(chan int)},
		{"struct", struct{ Field string }{"private"}},
		{"pointer", new(int)},
		{"nonstring key", map[int]string{1: "value"}},
		{"nonfinite", math.Inf(1)},
		{"nan", math.NaN()},
		{"unsafe signed", int64(9007199254740993)},
		{"unsafe unsigned", uint64(9007199254740993)},
		{"unsafe integral float", float64(9007199254740992)},
		{"unsafe number", json.Number("9007199254740993")},
		{"invalid number", json.Number("NaN")},
		{"invalid utf8", string([]byte{0xff})},
		{"cycle", cycle},
		{"nested invalid", []any{map[string]any{"bad": make(chan int)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := wire.NormalizeToolArgs(map[string]any{"secret": test.value})
			if !errors.Is(err, wire.ErrInvalidToolArgs) {
				t.Fatalf("want invalid-args error, got %v", err)
			}
		})
	}
	if _, err := wire.NormalizeToolArgs(map[string]any{string([]byte{0xff}): "value"}); !errors.Is(err, wire.ErrInvalidToolArgs) {
		t.Fatalf("invalid key accepted: %v", err)
	}
}

// Tool-call validation must not tighten conversion of tool OUTPUT or document DATA maps. Their
// existing invalid-content drop-to-nil behavior remains independently observable.
func TestOtherContentMapsRetainDropToNil(t *testing.T) {
	invalid := map[string]any{"unsupported": make(chan int)}
	result := wire.ToolResultFromProto(wire.ToolResultToProto(&api.ToolResult{ID: "call-1", Output: invalid}))
	if result.Output != nil {
		t.Fatalf("tool output behavior changed: %#v", result.Output)
	}
	message := &api.Message{Role: "assistant", Parts: []api.Part{{Data: invalid}}}
	got := wire.MessageFromProto(wire.MessageToProto(message))
	if len(got.Parts) != 1 || got.Parts[0].Data != nil {
		t.Fatalf("document data behavior changed: %#v", got)
	}
}
