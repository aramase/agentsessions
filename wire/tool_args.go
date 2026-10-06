package wire

import (
	"encoding"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"unicode/utf8"
)

// ErrInvalidToolArgs indicates arguments outside the JSON-shaped, lossless numeric domain of a
// tool call. It carries no argument values, since boundary errors may enter the session journal.
var ErrInvalidToolArgs = errors.New("wire: tool arguments must be JSON-shaped with finite, safe-precision numbers")

// NormalizeToolArgs validates tool-call arguments and returns their protobuf Struct representation
// as a Go map. Object order is ignored; Go types are interchangeable only when the existing Struct
// conversion yields the same values. Its whole-map JSON fallback can turn nested nil containers
// into null when typed values are present. Preserve those recorded null/empty distinctions rather
// than change legacy serialization. Other content maps keep the permissive toStruct conversion.
func NormalizeToolArgs(args map[string]any) (map[string]any, error) {
	if args == nil {
		return nil, nil
	}
	// Marshal rejects cycles and unsupported values before recursively checking the shape. Do not
	// return its error: a custom marshaler or unsupported value may expose argument contents.
	if _, err := json.Marshal(args); err != nil {
		return nil, ErrInvalidToolArgs
	}
	if !toolArgShape(reflect.ValueOf(args)) {
		return nil, ErrInvalidToolArgs
	}
	s := toStruct(args)
	if s == nil {
		return nil, ErrInvalidToolArgs
	}
	return s.AsMap(), nil
}

const maxSafeToolInteger = 1<<53 - 1

func toolArgShape(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	// Custom serializers need not preserve the inspected shape and can hide cycles from Marshal.
	// Tool args are values, not arbitrary objects with their own serialization behavior.
	if _, ok := v.Interface().(json.Marshaler); ok {
		return false
	}
	if _, ok := v.Interface().(encoding.TextMarshaler); ok {
		return false
	}
	if n, ok := v.Interface().(json.Number); ok {
		f, err := n.Float64()
		return err == nil && safeToolNumber(f)
	}
	switch v.Kind() {
	case reflect.Interface:
		return v.IsNil() || toolArgShape(v.Elem())
	case reflect.Bool:
		return true
	case reflect.String:
		return utf8.ValidString(v.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := v.Int()
		return n >= -maxSafeToolInteger && n <= maxSafeToolInteger
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() <= maxSafeToolInteger
	case reflect.Float32, reflect.Float64:
		return safeToolNumber(v.Float())
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return false
		}
		iter := v.MapRange()
		for iter.Next() {
			if !toolArgShape(iter.Key()) || !toolArgShape(iter.Value()) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if !toolArgShape(v.Index(i)) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func safeToolNumber(n float64) bool {
	return !math.IsNaN(n) && !math.IsInf(n, 0) && (math.Trunc(n) != n || math.Abs(n) <= maxSafeToolInteger)
}
