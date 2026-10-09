//go:build ignore

// Run from the current module after generate.go has run against v0.1.2.
package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
)

func main() {
	if len(os.Args) != 2 {
		panic("usage: go run generate-mixed.go FIXTURE_DIRECTORY")
	}
	for _, interrupted := range []bool{false, true} {
		data, err := os.ReadFile(filepath.Join(os.Args[1], "crashed-output.json"))
		if err != nil {
			panic(err)
		}
		var records []eventlog.Record
		if err := json.Unmarshal(data, &records); err != nil {
			panic(err)
		}
		log := eventlog.AsStore(eventlog.New())
		fence, err := log.NewFence()
		if err != nil {
			panic(err)
		}
		for _, record := range records {
			if _, err := log.Append(record.Seq-1, fence, record.Event); err != nil {
				panic(err)
			}
		}
		c, err := controller.New(log, echoagent.Model, controller.WithSessionUID("v012-fixture-session"))
		if err != nil {
			panic(err)
		}
		if resumed, err := c.Resume(context.Background(), echoagent.Harness{}); !resumed || err != nil {
			panic("legacy recovery failed")
		}
		// Persist the actual mid-turn upgrade before a modern execution is added.
		if !interrupted {
			write(log, os.Args[1], "upgraded-resume")
		}
		c, err = controller.New(log, echoagent.Model, controller.WithSessionUID("v012-fixture-session"), controller.WithObserver(controller.Observer{
			OnRecord: func(record eventlog.Record) {
				if interrupted && record.Event.Kind == api.EventOutput {
					panic("simulated process death")
				}
			},
		}))
		if err != nil {
			panic(err)
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
			if err := c.Exec(context.Background(), echoagent.Harness{}, []api.Message{*api.TextMessage("user", "new")}, head); err != nil {
				panic(err)
			}
		}()
		name := "mixed-completed"
		if interrupted {
			name = "mixed-interrupted"
		}
		write(log, os.Args[1], name)
	}
}

func write(log eventlog.Store, directory, name string) {
	if err := log.Verify(); err != nil {
		panic(err)
	}
	records, err := log.Read(1)
	if err != nil {
		panic(err)
	}
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(directory, name+".json"), append(data, '\n'), 0o644); err != nil {
		panic(err)
	}
}
