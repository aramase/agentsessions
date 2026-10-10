package harnesswire

import (
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
)

func TestDescriptorVersionRoundTrip(t *testing.T) {
	for _, version := range []string{"", "replay-v1"} {
		t.Run(version, func(t *testing.T) {
			desc := api.Descriptor{ID: "implementation", Version: version, Models: []string{"model"}, Tools: []api.ToolSpec{{Name: "tool", Mediation: api.MediationControllerMediated}}, Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}
			encoded := DescriptorToProto(desc)
			if encoded.GetVersion() != version {
				t.Fatalf("wire version = %q, want %q", encoded.GetVersion(), version)
			}
			if got := descriptorFromProto(encoded); !reflect.DeepEqual(got, desc) {
				t.Fatalf("round-trip = %#v, want %#v", got, desc)
			}
		})
	}
}
