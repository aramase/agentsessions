package session

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/sqlitelog"
)

// Caller-supplied session metadata is stored as JSON rather than as a column per field. The store
// has no reason to interpret it: these values describe a session, they do not control it, and a
// shape that round-trips exactly is worth more than one that is queryable. Labels become worth
// indexing when something actually selects on them.
//
// Empty values are stored as the empty string rather than "{}" or "null", so "was anything
// attached?" stays answerable by looking at the column.

// encodeMessage renders a proto message for storage, or "" when it carries nothing.
func encodeMessage(m proto.Message) (string, error) {
	if m == nil || !m.ProtoReflect().IsValid() {
		return "", nil
	}
	if proto.Size(m) == 0 {
		return "", nil
	}
	b, err := protojson.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("encode %T: %w", m, err)
	}
	return string(b), nil
}

// decodeMessage restores a stored proto message. A row that cannot be parsed is reported rather
// than silently dropped: returning a session with its metadata quietly missing would be a worse
// failure than refusing to return it, because the caller could not tell.
func decodeMessage(stored string, into proto.Message) error {
	if stored == "" {
		return nil
	}
	if err := protojson.Unmarshal([]byte(stored), into); err != nil {
		return fmt.Errorf("decode %T: %w", into, err)
	}
	return nil
}

// encodeMap renders a label or annotation map, or "" when empty.
func encodeMap(m map[string]string) (string, error) {
	if len(m) == 0 {
		return "", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("encode map: %w", err)
	}
	return string(b), nil
}

// decodeMap restores a stored map.
func decodeMap(stored string) (map[string]string, error) {
	if stored == "" {
		return nil, nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(stored), &m); err != nil {
		return nil, fmt.Errorf("decode map: %w", err)
	}
	return m, nil
}

// metadataFromSpec extracts the caller-supplied metadata a session carries.
//
// Identity is recorded, never checked. It is provenance: it says on whose behalf a session was
// created, and the hash chain makes that record tamper-evident. It is not authorization, and
// nothing in this implementation treats it as such. See docs/security.md.
func metadataFromSpec(spec *v1.Session) (labels, annotations, origin, identity string, err error) {
	if labels, err = encodeMap(spec.GetLabels()); err != nil {
		return "", "", "", "", err
	}
	if annotations, err = encodeMap(spec.GetAnnotations()); err != nil {
		return "", "", "", "", err
	}
	if origin, err = encodeMessage(spec.GetOrigin()); err != nil {
		return "", "", "", "", err
	}
	if identity, err = encodeMessage(spec.GetIdentity()); err != nil {
		return "", "", "", "", err
	}
	return labels, annotations, origin, identity, nil
}

// applyMetadata fills the stored metadata onto a response.
func applyMetadata(out *v1.Session, info sqlitelog.SessionInfo) error {
	var err error
	if out.Labels, err = decodeMap(info.Labels); err != nil {
		return err
	}
	if out.Annotations, err = decodeMap(info.Annotations); err != nil {
		return err
	}
	if info.Origin != "" {
		origin := &v1.Origin{}
		if err := decodeMessage(info.Origin, origin); err != nil {
			return err
		}
		out.Origin = origin
	}
	if info.Identity != "" {
		identity := &v1.IdentityRef{}
		if err := decodeMessage(info.Identity, identity); err != nil {
			return err
		}
		out.Identity = identity
	}
	return nil
}
