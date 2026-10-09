//go:build ignore

// Run from a checkout of v0.1.2, not from the current module; see README.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
)

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		panic("usage: go run generate.go OUTPUT_DIRECTORY [FIXTURE_NAME]")
	}
	for _, fixture := range []struct {
		name   string
		inputs []string
		crash  api.EventKind
	}{
		{"completed", []string{"hello"}, ""},
		{"crashed-input", []string{"hello"}, api.EventInput},
		{"crashed-output", []string{"hello"}, api.EventOutput},
		{"crashed-model", []string{"hello"}, api.EventModelCall},
		{"multi-turn", []string{"first", "second"}, ""},
		{"multi-turn-crashed-output", []string{"first", "second"}, api.EventOutput},
		{"model-error", []string{"hello"}, ""},
	} {
		if len(os.Args) == 3 && os.Args[2] != fixture.name {
			continue // regenerate a new fixture without changing frozen random IDs in older ones
		}
		log := eventlog.AsStore(eventlog.New())
		model := echoagent.Model
		modelError := errors.New("fixture model failure")
		if fixture.name == "model-error" {
			model = func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
				return api.ModelResponse{}, modelError
			}
		}
		c, err := controller.New(log, model, controller.WithObserver(controller.Observer{
			OnRecord: func(record eventlog.Record) {
				if record.Event.Kind == fixture.crash {
					panic("simulated process death")
				}
			},
		}))
		if err != nil {
			panic(err)
		}
		for i, text := range fixture.inputs {
			// Only the final turn crashes; all prior turns commit normally.
			crash := fixture.crash
			if i < len(fixture.inputs)-1 {
				fixture.crash = ""
			}
			func() {
				defer func() {
					if value := recover(); value != nil && value != "simulated process death" {
						panic(value)
					}
				}()
				head, err := log.Head()
				if err != nil {
					panic(err)
				}
				err = c.Exec(context.Background(), echoagent.Harness{}, []api.Message{*api.TextMessage("user", text)}, head)
				if fixture.name == "model-error" {
					if !errors.Is(err, modelError) {
						panic(fmt.Sprintf("model-error Exec = %v", err))
					}
				} else if err != nil {
					panic(err)
				}
			}()
			fixture.crash = crash
		}
		records, err := log.Read(1)
		if err != nil {
			panic(err)
		}
		if err := log.Verify(); err != nil {
			panic(err)
		}
		data, err := json.MarshalIndent(records, "", "  ")
		if err != nil {
			panic(err)
		}
		path := filepath.Join(os.Args[1], fixture.name+".json")
		if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
			panic(err)
		}
		fmt.Printf("%s: %d records\n", fixture.name, len(records))
	}
}
