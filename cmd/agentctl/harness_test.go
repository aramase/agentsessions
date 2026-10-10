package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHarnessSpecParsingRejectsUnknownAndMalformedJSON(t *testing.T) {
	for _, body := range []string{`{`, `{"remote":{"address":"127.0.0.1:1"},"unknown":"data"}`} {
		path := filepath.Join(t.TempDir(), "spec.json")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		err := cmdHarness([]string{"register", "--server", "127.0.0.1:1", "--name", "mine", "--spec", path})
		if err == nil || !strings.Contains(err.Error(), "parse HarnessSpec") {
			t.Fatalf("body %s: got %v, want strict JSON parse error before dialing", body, err)
		}
	}
}

func TestHarnessCommandRequiresOperatorServer(t *testing.T) {
	for _, args := range [][]string{
		{"register", "--name", "mine", "--spec", "spec.json"},
		{"get", "--name", "mine"}, {"list"}, {"retire", "--name", "mine"},
	} {
		if err := cmdHarness(args); err == nil || !strings.Contains(err.Error(), "--server") {
			t.Errorf("harness %v: got %v, want --server usage error", args, err)
		}
	}
	for _, args := range [][]string{{}, {"bad", "--server", "127.0.0.1:1"}, {"get", "--server", "127.0.0.1:1"}, {"register", "--server", "127.0.0.1:1", "--name", "mine"}} {
		if err := cmdHarness(args); err == nil {
			t.Errorf("harness %v accepted missing arguments", args)
		}
	}
}
