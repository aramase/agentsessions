// Package canon defines the language-neutral canonical serialization for the event log's
// tamper-evident hash-chain. content_hash is defined over RFC 8785 (JCS) applied to the
// proto3-JSON mapping of the event, so ANY implementation — not only this Go host — can
// recompute and independently verify the chain. That independent verifiability is the
// "provenance anyone can audit" property (google/ax has no log hash-chain at all). See the
// determinism contract §7.
//
//	content_hash = lc-hex( SHA-256( JCS({
//	    "event":     proto3-JSON(Event),
//	    "prev_hash": <hex>,
//	    "seq":       <int64-as-string>,
//	}) ) )
//
// proto3-JSON maps int64→string, bytes→base64, enum→name; JCS then sorts keys and canonicalizes
// numbers/strings/whitespace. Because the event is serialized via proto3-JSON, structured content
// maps must be JSON-shaped (no int64 > 2^53) — see package wire.
package canon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/gowebpki/jcs"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/wire"
)

// marshalOpts pins the proto3-JSON emission so the canonical form is identical across
// implementations *before* JCS runs. proto3-JSON field presence and naming are not normalized by
// JCS, so they must be fixed here: proto field names (snake_case — the stable contract names,
// matching google/ax's own protojson usage), enums as names, and unpopulated fields omitted
// (standard proto3-JSON). JCS then normalizes key order, number/string forms, and whitespace.
var marshalOpts = protojson.MarshalOptions{
	UseProtoNames:   true,
	UseEnumNumbers:  false,
	EmitUnpopulated: false,
}

// Event returns the canonical bytes of a single event: JCS applied to its proto3-JSON form.
// Useful anywhere a stable, cross-implementation event fingerprint is needed (e.g. a model
// input hash).
func Event(ev api.Event) ([]byte, error) {
	ej, err := marshalOpts.Marshal(wire.EventToProto(ev))
	if err != nil {
		return nil, err
	}
	return jcs.Transform(ej)
}

// Record returns the canonical bytes that content_hash is computed over: the event plus the
// chain envelope (prev_hash, seq), canonicalized as one JCS object. seq is encoded as a string
// (proto3-JSON's int64 convention) so it is exact regardless of a consumer's JSON number type.
func Record(prevHash string, seq int64, ev api.Event) ([]byte, error) {
	ej, err := marshalOpts.Marshal(wire.EventToProto(ev))
	if err != nil {
		return nil, err
	}
	envelope := map[string]any{
		"event":     json.RawMessage(ej),
		"prev_hash": prevHash,
		"seq":       strconv.FormatInt(seq, 10),
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	return jcs.Transform(raw)
}

// HashRecord is content_hash: lowercase-hex SHA-256 over the canonical record bytes. At a fork,
// a child's first record uses prev_hash = parent@R.content_hash, turning the chain into a tree.
func HashRecord(prevHash string, seq int64, ev api.Event) (string, error) {
	b, err := Record(prevHash, seq, ev)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
