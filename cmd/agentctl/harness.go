package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const harnessRPCTimeout = 30 * time.Second // Get --observe is bounded by 10 seconds on the host.

func cmdHarness(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: agentctl harness <register|get|list|retire> --server <operator-address> [flags]")
	}
	op := args[0]
	if op != "register" && op != "get" && op != "list" && op != "retire" {
		return fmt.Errorf("unknown harness command %q; want register, get, list or retire", op)
	}
	fs := flag.NewFlagSet("harness "+op, flag.ContinueOnError)
	var server, name, specPath, reason string
	var observe, includeRetired bool
	var pageSize int
	fs.StringVar(&server, "server", "", "operator HarnessRegistry address (required)")
	switch op {
	case "register":
		fs.StringVar(&name, "name", "", "harness name (DNS label)")
		fs.StringVar(&specPath, "spec", "", "HarnessSpec proto-JSON file")
	case "get":
		fs.StringVar(&name, "name", "", "harness name")
		fs.BoolVar(&observe, "observe", false, "ask the harness to Describe itself")
	case "list":
		fs.BoolVar(&includeRetired, "include-retired", false, "include retired harnesses")
		fs.IntVar(&pageSize, "page-size", 0, "entries per request; 0 uses the server default")
	case "retire":
		fs.StringVar(&name, "name", "", "harness name")
		fs.StringVar(&reason, "reason", "", "reason for retirement")
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("harness %s: unexpected argument %q", op, fs.Arg(0))
	}
	if server == "" {
		return fmt.Errorf("harness %s: --server <operator-address> is required (no embedded registry)", op)
	}
	if op != "list" && name == "" {
		return fmt.Errorf("harness %s: --name is required", op)
	}
	if op == "register" && specPath == "" {
		return errors.New("harness register: --spec is required")
	}
	if op == "list" && pageSize < 0 {
		return errors.New("harness list: --page-size must not be negative")
	}
	// The service clamps requests above 500. Clamp before narrowing to the proto's int32
	// field too, so a large positive CLI value cannot wrap into a negative request.
	if op == "list" && pageSize > 500 {
		pageSize = 500
	}
	var spec *v1.HarnessSpec
	if op == "register" {
		data, err := os.ReadFile(specPath)
		if err != nil {
			return fmt.Errorf("read spec %s: %w", specPath, err)
		}
		spec = &v1.HarnessSpec{}
		if err := protojson.Unmarshal(data, spec); err != nil {
			return fmt.Errorf("parse HarnessSpec %s: %w", specPath, err)
		}
	}
	conn, err := grpc.NewClient(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("operator server %s: %w", server, err)
	}
	defer conn.Close()
	c := v1.NewHarnessRegistryClient(conn)
	var out proto.Message
	switch op {
	case "register":
		ctx, cancel := context.WithTimeout(context.Background(), harnessRPCTimeout)
		defer cancel()
		out, err = c.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: name, Spec: spec})
	case "get":
		ctx, cancel := context.WithTimeout(context.Background(), harnessRPCTimeout)
		defer cancel()
		out, err = c.GetHarness(ctx, &v1.GetHarnessRequest{Name: name, Observe: observe})
	case "retire":
		ctx, cancel := context.WithTimeout(context.Background(), harnessRPCTimeout)
		defer cancel()
		out, err = c.RetireHarness(ctx, &v1.RetireHarnessRequest{Name: name, Reason: reason})
	case "list":
		all := &v1.ListHarnessesResponse{}
		token := ""
		for {
			ctx, cancel := context.WithTimeout(context.Background(), harnessRPCTimeout)
			page, callErr := c.ListHarnesses(ctx, &v1.ListHarnessesRequest{PageSize: int32(pageSize), PageToken: token, IncludeRetired: includeRetired})
			cancel()
			if callErr != nil {
				return callErr
			}
			all.Harnesses = append(all.Harnesses, page.GetHarnesses()...)
			token = page.GetNextPageToken()
			if token == "" {
				break
			}
		}
		out = all
	}
	if err != nil {
		return err
	}
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(out)
	if err != nil {
		return fmt.Errorf("encode harness response: %w", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, string(data)); err != nil {
		return fmt.Errorf("harness %s RPC succeeded but writing JSON response to stdout failed: %w", op, err)
	}
	return nil
}
