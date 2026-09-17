package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/harness/chatagent"
)

func TestSelectHarness(t *testing.T) {
	for _, tt := range []struct {
		name         string
		kind         string
		model        string
		id           string
		resumability api.Resumability
	}{
		{"default", "", "", "echo", api.ResumabilityStatelessReplay},
		{"echo", "echo", "ignored", "echo", api.ResumabilityStatelessReplay},
		{"counter", "counter", "", "counter", api.ResumabilityRequiresMemorySnapshot},
		{"existing fallback", "unknown", "", "echo", api.ResumabilityStatelessReplay},
		{"chat", "chat", "test-model", "chat", api.ResumabilityStatelessReplay},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HARNESS_KIND", tt.kind)
			t.Setenv("HARNESS_MODEL", tt.model)
			harness, err := selectHarness()
			if err != nil {
				t.Fatal(err)
			}
			desc, err := harness.Describe(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if desc.ID != tt.id || desc.Capabilities.Resumability != tt.resumability {
				t.Fatalf("descriptor = %+v, want %s with %s", desc, tt.id, tt.resumability)
			}
			if tt.kind == "chat" {
				chat, ok := harness.(chatagent.Harness)
				if !ok || chat.Model != tt.model {
					t.Fatalf("harness = %+v, want chat with configured model", harness)
				}
				if !desc.Capabilities.ForkSafe || !reflect.DeepEqual(desc.Models, []string{tt.model}) {
					t.Fatalf("chat descriptor = %+v", desc)
				}
			}
		})
	}
}

func TestSelectChatRequiresModel(t *testing.T) {
	t.Setenv("HARNESS_KIND", "chat")
	t.Setenv("HARNESS_MODEL", "")
	harness, err := selectHarness()
	if err == nil || !strings.Contains(err.Error(), "HARNESS_MODEL is required") {
		t.Fatalf("error = %v, want missing HARNESS_MODEL", err)
	}
	if harness != nil {
		t.Fatal("invalid chat configuration returned a fallback harness")
	}
}
