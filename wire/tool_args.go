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
	// Inspect before Marshal so rejected values cannot execute custom serializers. Track only
	// containers on the current path: shared acyclic values are valid, back-references are not.
	if !toolArgShape(reflect.ValueOf(args), make(map[toolArgContainer]bool)) {
		return nil, ErrInvalidToolArgs
	}
	if _, err := json.Marshal(args); err != nil {
		return nil, ErrInvalidToolArgs
	}
	s := toStruct(args)
	if s == nil {
		return nil, ErrInvalidToolArgs
	}
	return s.AsMap(), nil
}

const maxSafeToolInteger = 1<<53 - 1

type toolArgContainer struct {
	typ reflect.Type
	ptr uintptr
	len int
}

func toolArgShape(v reflect.Value, visiting map[toolArgContainer]bool) bool {
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
		return v.IsNil() || toolArgShape(v.Elem(), visiting)
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
	case reflect.Map, reflect.Slice:
		if v.Kind() == reflect.Map && v.Type().Key().Kind() != reflect.String {
			return false
		}
		if v.IsNil() {
			return true
		}
		container := toolArgContainer{typ: v.Type(), ptr: v.Pointer(), len: v.Len()}
		if visiting[container] {
			return false
		}
		visiting[container] = true
		defer delete(visiting, container)
		if v.Kind() == reflect.Map {
			iter := v.MapRange()
			for iter.Next() {
				if !toolArgShape(iter.Key(), visiting) || !toolArgShape(iter.Value(), visiting) {
					return false
				}
			}
			return true
		}
		fallthrough
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if !toolArgShape(v.Index(i), visiting) {
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
