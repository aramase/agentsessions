// Package client is the Go client for the Sessions API.
//
// It wraps the generated gRPC stubs with the bookkeeping every caller would otherwise write for
// itself: dialing, reading the session frame off an Exec stream, walking pagination, and draining
// streams to completion. Before it existed, agentctl and the langflow adapter had each reimplemented
// the same loops, which is a sign the contract was leaving work on the caller's side of the line.
//
// It is a convenience over the contract, never a gate in front of it. Sessions returns the
// underlying stub, so anything the SDK does not model is still reachable without abandoning it.
//
// The SDK adds no auth or transport security, because the reference implementation has none: see
// docs/security.md before pointing this at anything you do not control the network around.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/observability"
	"github.com/aramase/agentsessions/wire"
)

// Client talks to a Sessions server.
type Client struct {
	conn    *grpc.ClientConn
	stub    v1.SessionsClient
	project string
}

// Option configures a Client.
type Option func(*config)

type config struct {
	project  string
	dialOpts []grpc.DialOption
}

// WithProject sets the project used by calls that take one and were given none. It is a default for
// convenience, not an access boundary: project is an exact-match filter, and any caller may name any
// project. See docs/security.md.
func WithProject(p string) Option { return func(c *config) { c.project = p } }

// WithDialOptions appends gRPC dial options, for credentials, interceptors, or a custom dialer.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(c *config) { c.dialOpts = append(c.dialOpts, opts...) }
}

// Dial connects to a Sessions server.
//
// The connection is insecure unless WithDialOptions supplies credentials, which matches what the
// server currently offers: nothing in the reference implementation terminates TLS.
func Dial(target string, opts ...Option) (*Client, error) {
	cfg := config{}
	for _, o := range opts {
		o(&cfg)
	}
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(observability.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(observability.StreamClientInterceptor),
	}
	dialOpts = append(dialOpts, cfg.dialOpts...)

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", target, err)
	}
	return &Client{conn: conn, stub: v1.NewSessionsClient(conn), project: cfg.project}, nil
}

// Close releases the connection.
func (c *Client) Close() error { return c.conn.Close() }

// Sessions returns the underlying stub, so the SDK is a convenience rather than a ceiling.
func (c *Client) Sessions() v1.SessionsClient { return c.stub }

// CreateSession registers a session without running anything. It is only needed to set metadata the
// server cannot infer; Exec creates one on its own otherwise.
func (c *Client) CreateSession(ctx context.Context, s *v1.Session) (*v1.Session, error) {
	if s == nil {
		s = &v1.Session{}
	}
	if s.GetMetadata().GetProject() == "" && c.project != "" {
		if s.Metadata == nil {
			s.Metadata = &v1.ResourceMetadata{}
		}
		s.Metadata.Project = c.project
	}
	return c.stub.CreateSession(ctx, &v1.CreateSessionRequest{Session: s})
}

// GetSession returns a session's metadata and current log cursor.
func (c *Client) GetSession(ctx context.Context, uid string) (*v1.Session, error) {
	return c.stub.GetSession(ctx, &v1.GetSessionRequest{Uid: uid})
}

// ListSessions returns every session in a project, newest first, walking pagination internally.
//
// Walking every page is the default because stopping at the first one silently under-reports, which
// is a worse failure than being slow: a caller that does not know pagination exists gets a truthful
// answer rather than a partial one. Use ListSessionsPage for explicit control.
func (c *Client) ListSessions(ctx context.Context, project string) ([]*v1.Session, error) {
	if project == "" {
		project = c.project
	}
	var out []*v1.Session
	token := ""
	for {
		page, err := c.ListSessionsPage(ctx, project, 0, token)
		if err != nil {
			return nil, err
		}
		out = append(out, page.GetSessions()...)
		token = page.GetNextPageToken()
		if token == "" {
			return out, nil
		}
	}
}

// ListSessionsPage returns one page. pageSize 0 uses the server default; an empty token starts at
// the first page.
func (c *Client) ListSessionsPage(ctx context.Context, project string, pageSize int32, token string) (*v1.ListSessionsResponse, error) {
	if project == "" {
		project = c.project
	}
	return c.stub.ListSessions(ctx, &v1.ListSessionsRequest{
		Project:   project,
		PageSize:  pageSize,
		PageToken: token,
	})
}

// ExecOptions describes one turn.
type ExecOptions struct {
	// Session to run against. Empty creates one, whose uid comes back on TurnResult.Session.
	Session string
	// Inputs for this turn. Empty re-drives an interrupted execution with no new input.
	Inputs []string
	// Harness overrides the session's configured harness for this turn.
	Harness string
	// ExpectedLastSeq opts into the single-writer compare-and-swap. Nil appends at the current
	// head. A stale value comes back as codes.Aborted rather than interleaving silently.
	ExpectedLastSeq *int64
	// OnSession is called when the session frame arrives, which is before the turn runs. A caller
	// that created its session implicitly learns the uid here rather than after the turn, which
	// matters when the turn is long or fails partway.
	OnSession func(*v1.Session)
	// OnRecord is called for each committed record as it arrives, for callers that want to react
	// during the turn rather than after it.
	OnRecord func(*v1.LogRecord)
	// OnDelta is called for each streaming chunk. The server does not emit deltas yet, so this is
	// wired for when it does rather than exercised today.
	OnDelta func(*v1.Delta)
}

// TurnResult is one completed turn.
type TurnResult struct {
	// Session as reported by the stream's first frame. For a turn that created its session, this
	// is where the uid comes from.
	Session *v1.Session
	// Records committed by this turn, in order.
	Records []*v1.LogRecord
	// Output is the assistant text this turn committed. It reads the log rather than accumulating
	// streamed chunks, so it reflects what was durably recorded.
	Output string
	// LastSeq is the log head after the turn. Feed it to the next turn's ExpectedLastSeq to keep
	// the strict single-writer check without a separate GetSession.
	LastSeq int64
}

// Exec runs one turn and returns when it completes.
//
// It reads the stream to completion, which is what the contract requires: every Exec stream opens
// with the session frame, so an execution error arrives after it and a caller that reads a single
// frame would see the session instead of the failure.
func (c *Client) Exec(ctx context.Context, opts ExecOptions) (*TurnResult, error) {
	req := &v1.ExecRequest{
		Session:         opts.Session,
		Harness:         opts.Harness,
		ExpectedLastSeq: opts.ExpectedLastSeq,
	}
	for _, in := range opts.Inputs {
		req.Inputs = append(req.Inputs, wire.MessageToProto(api.TextMessage("user", in)))
	}

	stream, err := c.stub.Exec(ctx, req)
	if err != nil {
		return nil, err
	}

	result := &TurnResult{}
	var output string
	for {
		update, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// The session frame arrives before the turn runs, so a failed execution still reports
			// which session it was against. Returning it alongside the error is what lets a caller
			// find (or clean up) a session that Exec created and then could not run.
			return result, err
		}
		switch {
		case update.GetSession() != nil:
			result.Session = update.GetSession()
			if opts.OnSession != nil {
				opts.OnSession(result.Session)
			}
		case update.GetRecord() != nil:
			rec := update.GetRecord()
			result.Records = append(result.Records, rec)
			if rec.GetSeq() > result.LastSeq {
				result.LastSeq = rec.GetSeq()
			}
			if ev := rec.GetEvent(); ev.GetKind() == v1.EventKind_EVENT_OUTPUT {
				if msg := wire.MessageFromProto(ev.GetMessage()); msg != nil {
					output += msg.Text()
				}
			}
			if opts.OnRecord != nil {
				opts.OnRecord(rec)
			}
		case update.GetDelta() != nil:
			if opts.OnDelta != nil {
				opts.OnDelta(update.GetDelta())
			}
		}
	}
	result.Output = output
	if result.LastSeq == 0 {
		result.LastSeq = result.Session.GetLastSeq()
	}
	return result, nil
}

// Replay re-delivers a session's committed records. It is read-only: no model is invoked and no
// event is appended.
func (c *Client) Replay(ctx context.Context, uid string, fromSeq, toSeq int64) ([]*v1.LogRecord, error) {
	stream, err := c.stub.Replay(ctx, &v1.ReplayRequest{Session: uid, FromSeq: fromSeq, ToSeq: toSeq})
	if err != nil {
		return nil, err
	}
	var out []*v1.LogRecord
	for {
		rec, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
}

// ForkOptions describes a fan-out.
type ForkOptions struct {
	// AtSeq is the branch point. 0 forks at the head, which is the only point a memory-snapshot
	// harness can fork from.
	AtSeq int64
	// Count is how many children to create. 0 means one.
	Count int32
	// Names labels the children in order. It must be empty or exactly Count entries.
	Names []string
}

// Fork branches a session into one or more children, each continuing independently from the same
// point. Children inherit the parent's project, harness, and model, but not its name.
func (c *Client) Fork(ctx context.Context, uid string, opts ForkOptions) ([]*v1.Session, error) {
	resp, err := c.stub.Fork(ctx, &v1.ForkRequest{
		Session:    uid,
		AtSeq:      opts.AtSeq,
		Count:      opts.Count,
		ChildNames: opts.Names,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetChildren(), nil
}

// Suspend frees the session's compute, recording the transition on its chain.
func (c *Client) Suspend(ctx context.Context, uid string) (*v1.Session, error) {
	return c.stub.Suspend(ctx, &v1.SuspendRequest{Session: uid})
}

// Resume brings a suspended session back. boot cold-boots and replays instead of restoring a
// snapshot.
func (c *Client) Resume(ctx context.Context, uid string, boot bool) (*v1.Session, error) {
	return c.stub.Resume(ctx, &v1.ResumeRequest{Session: uid, Boot: boot})
}
