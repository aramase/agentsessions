package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
)

func cmdApprove(args []string) error { return approval(args, true) }
func cmdDeny(args []string) error    { return approval(args, false) }

// Both commands persist the decision before recovery. Failure to resume must never be
// presented as failure to commit, or invite changing a decision that is already durable.
func approval(args []string, approved bool) error {
	which := "deny"
	if approved {
		which = "approve"
	}
	fs := flag.NewFlagSet(which, flag.ContinueOnError)
	cfg := commonFlags(fs)
	var uid, execution, call, reason, actor, issuer, subject string
	var requestSeq int64
	var jsonOutput bool
	fs.StringVar(&uid, "session", "", "session UID (or supply one positional UID)")
	fs.StringVar(&execution, "execution", "", "execution ID; provide with --tool-call and --request-seq, otherwise discover pending approval")
	fs.StringVar(&call, "tool-call", "", "tool call ID; requires the complete explicit tuple")
	fs.Int64Var(&requestSeq, "request-seq", 0, "positive host approval request sequence; requires the complete explicit tuple")
	fs.StringVar(&reason, "reason", "", "reason recorded with the decision")
	fs.StringVar(&actor, "actor", "", "identity principal recorded as provenance, not authorization")
	fs.StringVar(&issuer, "identity-issuer", "", "identity issuer recorded as provenance")
	fs.StringVar(&subject, "identity-subject", "", "identity subject recorded as provenance")
	fs.BoolVar(&jsonOutput, "json", false, "print the original committed decision and resumed session as protobuf JSON")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: agentctl %s [flags] <session UID>\nCommits a decision, then calls Resume. A recovery failure leaves the decision committed.\n", which)
		fs.PrintDefaults()
	}
	// The standard flag parser stops at a positional argument; allow the common UID-first
	// spelling without replacing the CLI framework or affecting other commands.
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		args = append(append([]string{}, args[1:]...), args[0])
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() > 1 || fs.NArg() == 1 && uid != "" {
		return fmt.Errorf("supply exactly one session UID, positional or --session")
	}
	if fs.NArg() == 1 {
		uid = fs.Arg(0)
	}
	if uid == "" {
		return fmt.Errorf("session UID is required")
	}
	tupleFlags := 0
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "execution" || f.Name == "tool-call" || f.Name == "request-seq" {
			tupleFlags++
		}
	})
	if tupleFlags != 0 && (tupleFlags != 3 || execution == "" || call == "" || requestSeq <= 0) {
		return fmt.Errorf("provide the full --execution, --tool-call and positive --request-seq tuple, or omit all three to discover pending approval")
	}
	c, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	ctx := context.Background()
	if tupleFlags == 0 {
		sess, err := c.GetSession(ctx, uid)
		if err != nil {
			return err
		}
		ref := sess.GetPendingApproval()
		if ref == nil {
			return fmt.Errorf("session %q has no pending approval; if a decision was already committed, use Resume (agentctl resume --session %s), or provide the full tuple for an exact retry", uid, uid)
		}
		execution, call, requestSeq = ref.GetExecutionId(), ref.GetToolCallId(), ref.GetRequestSeq()
	}
	response, err := c.Approve(ctx, uid, api.ApprovalDecision{
		ExecutionID: execution, ToolCallID: call, RequestSeq: requestSeq, Approved: approved, Reason: reason,
		Identity: api.IdentityRef{Principal: actor, Issuer: issuer, Subject: subject},
	})
	if err != nil {
		return err
	}
	current, err := c.Resume(ctx, uid, false)
	if err != nil {
		return fmt.Errorf("decision committed at seq %d for session %s; Resume failed: %w", response.GetDecision().GetSeq(), uid, err)
	}
	if jsonOutput {
		data, err := protojson.Marshal(&v1.ApproveResponse{Decision: response.GetDecision(), Session: current})
		if err != nil {
			return err
		}
		fmt.Println(string(data))
	} else {
		fmt.Printf("session %s decision committed seq=%d approved=%t compute=%s last_seq=%d\n", uid, response.GetDecision().GetSeq(), approved, current.GetComputeState(), current.GetLastSeq())
	}
	return nil
}
