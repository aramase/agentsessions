package eventlog_test

import (
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/eventlog"
)

func TestRecordRoundTrip(t *testing.T) {
	r := eventlog.Record{
		Seq: 7, PrevHash: "sha256:prev", Hash: "sha256:cur", Fence: 3,
		Event: api.Event{
			Kind:    api.EventOutput,
			Message: api.TextMessage("assistant", "hi"),
			Actor:   api.IdentityRef{Principal: "agent://a", Issuer: "entra", Subject: "s"},
		},
	}
	got := eventlog.RecordFromProto(eventlog.RecordToProto(r))
	if !reflect.DeepEqual(r, got) {
		t.Fatalf("record round-trip mismatch\n want: %#v\n got:  %#v", r, got)
	}
}
