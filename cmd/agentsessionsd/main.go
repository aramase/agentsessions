// Command agentsessionsd serves the Sessions API over TCP.
//
// Until now the Sessions service was only ever registered inside agentctl, on a per-process unix
// socket torn down when the command exits, so agentctl's --server flag had nothing to dial and a
// non-Go client had no way to reach a session at all. This is that missing entry point.
//
// This binary serves the reference echo harness by default and adds the conversational chat harness
// when -model is set, each on its own filesystem-only local backend. -harness name=address registers
// a STATELESS_REPLAY harness running elsewhere without rebuilding; it requires -model, so a remote
// harness's model calls never fall through to the built-in echo stub. A REQUIRES_MEMORY_SNAPSHOT
// harness needs a backend that owns its sandbox, which means building a server around such a backend.
// Unknown harnesses are refused rather than substituted.
//
// With -model set it drives a real OpenAI-compatible endpoint; without one it uses the built-in
// echo model, so the quickstart runs with no key. The API key comes from the environment rather
// than a flag, because a flag would put the credential in the process list and shell history.
//
// It is plaintext and unauthenticated: there is no authn, no authz, and no TLS anywhere in the
// reference implementation, and project is a filter rather than a tenancy boundary. Do not expose
// it to an untrusted network. See SECURITY.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/chatagent"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/internal/modelconfig"
	"github.com/aramase/agentsessions/internal/version"
	"github.com/aramase/agentsessions/model/openai"
	"github.com/aramase/agentsessions/observability"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/runtime/remote"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "agentsessionsd:", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:8080", "address to serve the Sessions API on")
	journal := flag.String("journal", "agentsessions.db", "sqlite journal path")
	project := flag.String("project", sqlitelog.DefaultProject, "default project (tenant)")
	model := flag.String("model", "", "model id for an OpenAI-compatible endpoint; empty uses the built-in echo model")
	modelBaseURL := flag.String("model-base-url", openai.DefaultBaseURL, "base URL of the OpenAI-compatible endpoint")
	modelPath := flag.String("model-path", openai.DefaultPath, "completions path under the base URL; may carry a query string")
	modelAuthHeader := flag.String("model-auth-header", "Authorization", "header carrying the credential from MODEL_API_KEY")
	remotes := remoteHarnesses{}
	flag.Var(remotes, "harness", "register a harness already running elsewhere, as name=address; repeatable")
	showVersion := flag.Bool("version", false, "print the build version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("agentsessionsd", version.Get())
		return nil
	}

	// Before any side effect: a misconfigured start must not leave a journal behind. Addresses are
	// already checked by remoteHarnesses.Set during flag parsing.
	if err := checkRemotes(*model, remotes); err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	store, err := sqlitelog.Open(*journal, sqlitelog.WithDefaultProject(*project))
	if err != nil {
		return fmt.Errorf("open journal %s: %w", *journal, err)
	}
	defer func() { _ = store.Close() }()

	modelFn, streamFn, modelDesc, err := modelFunc(*model, *modelBaseURL, *modelPath, *modelAuthHeader)
	if err != nil {
		return err
	}

	registry, closeBackends, err := harnessRegistry(*model, remotes, modelFn, streamFn, logger)
	if err != nil {
		return err
	}
	defer closeBackends()

	svc, err := sessionService(store, registry, logger, *project)
	if err != nil {
		return err
	}
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(observability.UnaryServerInterceptor(logger)),
		grpc.ChainStreamInterceptor(observability.StreamServerInterceptor(logger)),
	)
	v1.RegisterSessionsServer(srv, svc)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	logger.Info("agentsessionsd listening",
		"version", version.Get().Version,
		"addr", lis.Addr().String(),
		"journal", *journal,
		"project", *project,
		"harnesses", registry.Names(),
		"default_harness", registry.Default(),
		"model", modelDesc,
	)

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
		srv.GracefulStop()
		return nil
	case err := <-serveErr:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	}
}

// remoteHarnesses collects repeated -harness name=address flags. Registering a harness this way is
// the difference between adding one and rebuilding the server: the address points at a harness
// somebody else is already running, and the harness itself declares its resumability tier.
type remoteHarnesses map[string]string

func (r remoteHarnesses) String() string { return strings.Join(r.names(), ",") }

// names returns the registered names in sorted order, so anything derived from them (errors, logs)
// is deterministic.
func (r remoteHarnesses) names() []string {
	names := make([]string, 0, len(r))
	for name := range r {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r remoteHarnesses) Set(value string) error {
	name, addr, ok := strings.Cut(value, "=")
	name, addr = strings.TrimSpace(name), strings.TrimSpace(addr)
	if !ok || name == "" || addr == "" {
		return fmt.Errorf("want name=address, got %q", value)
	}
	if _, exists := r[name]; exists {
		return fmt.Errorf("harness %q already registered", name)
	}
	if err := checkHarnessAddress(addr); err != nil {
		return fmt.Errorf("harness %q: %w", name, err)
	}
	r[name] = addr
	return nil
}

// checkHarnessAddress refuses an address the harness dialers cannot reach, so an operator typo stops
// the server at startup instead of making every call on that harness fail as UNAVAILABLE, which
// looks like a recoverable outage. The accepted forms are the ones runtime/remote and placement dial:
//
//   - host:port, dialed over TCP; host is an IP address or a host name that is not also a gRPC
//     resolver scheme (passthrough:8080 would not be dialed as TCP), port is numeric
//   - dns:///host:port, or dns://resolver/host:port, resolved by gRPC's DNS resolver; resolver is
//     the DNS server to ask, an IP address or host name with an optional port
//   - unix:///absolute/path, or unix://relative/path, dialed as a path after "unix://"
//   - unix:path, absolute or relative, handed to gRPC's unix resolver
//
// Anything else, such as http://host:port, a bare host with no port, or a unix address with no path,
// is refused.
func checkHarnessAddress(addr string) error {
	const want = "want host:port, dns:///host:port, unix:///absolute/path or unix:relative/path"
	switch {
	case strings.HasPrefix(addr, "unix:"):
		p := strings.TrimPrefix(strings.TrimPrefix(addr, "unix:"), "//")
		// unix://// and unix:///. leave a path that is only the root (or current) directory, which
		// is never a socket.
		if p == "" || path.Clean(p) == "/" || path.Clean(p) == "." {
			return fmt.Errorf("address %q has no socket path; %s", addr, want)
		}
		return nil
	case strings.HasPrefix(addr, "dns:"):
		if err := checkDNSTarget(addr); err != nil {
			return fmt.Errorf("address %q: %w; %s", addr, err, want)
		}
		return nil
	case strings.Contains(addr, "://"):
		scheme, _, _ := strings.Cut(addr, "://")
		return fmt.Errorf("address %q uses unsupported scheme %q; %s", addr, scheme, want)
	}
	if err := checkHostPort(addr); err != nil {
		return fmt.Errorf("address %q: %w; %s", addr, err, want)
	}
	// gRPC parses a bare address as scheme:endpoint first and only falls back to DNS when no resolver
	// is registered for that scheme, so a host named like one (passthrough:8080) would not be dialed
	// as TCP host:port at all.
	if host, _, _ := net.SplitHostPort(addr); resolver.Get(strings.ToLower(host)) != nil {
		return fmt.Errorf("address %q: host %q is a gRPC resolver scheme, so the address would not be dialed as host:port; use dns:///%s", addr, host, addr)
	}
	return nil
}

// checkDNSTarget parses a dns: address the way gRPC does, so an address gRPC cannot dial is refused
// at startup rather than on the first call. gRPC runs url.Parse on the whole target; its DNS resolver
// then reads the URL path (or the opaque part, for dns:host:port) as the endpoint to resolve, and the
// URL host, when present, as the DNS server to ask, in the form ip, ip:port, host or host:port with
// port 53 by default.
//
// It is stricter than gRPC in two ways: the endpoint must carry a port, and userinfo, a query or a
// fragment, which gRPC's DNS resolver ignores, are refused rather than silently dropped.
func checkDNSTarget(addr string) error {
	u, err := url.Parse(addr)
	if err != nil {
		// url.Error repeats the address; keep only why it does not parse.
		if ue := (*url.Error)(nil); errors.As(err, &ue) {
			return ue.Err
		}
		return err
	}
	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return errors.New("a dns address takes only a resolver and host:port, not userinfo, a query or a fragment")
	}
	if u.Host != "" {
		if err := checkResolverAuthority(u.Host); err != nil {
			return fmt.Errorf("resolver %q: %w", u.Host, err)
		}
	}
	// gRPC's resolver.Target.Endpoint.
	endpoint := u.Path
	if endpoint == "" {
		endpoint = u.Opaque
	}
	endpoint = strings.TrimPrefix(endpoint, "/")
	if endpoint == "" {
		return errors.New("no host:port to resolve")
	}
	return checkHostPort(endpoint)
}

// checkResolverAuthority accepts the DNS server part of dns://resolver/host:port as gRPC's DNS
// resolver reads it: an IP address or a host name, with an optional port that defaults to 53.
func checkResolverAuthority(authority string) error {
	if _, err := netip.ParseAddr(authority); err == nil {
		return nil
	}
	host, port, err := net.SplitHostPort(authority)
	if err != nil {
		// No port: gRPC appends :53, which also accepts a bracketed IPv6 address.
		if host, port, err = net.SplitHostPort(authority + ":53"); err != nil {
			return errors.New("not an IP address or a host name, with an optional port")
		}
	}
	if host == "" {
		return errors.New("missing host")
	}
	if err := checkPort(port); err != nil {
		return err
	}
	return checkHost(host)
}

// checkHostPort accepts host:port where host is an IP address (IPv6 in brackets) or a host name, and
// port is a number from 1 to 65535.
func checkHostPort(hostport string) error {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		// SplitHostPort repeats the address; keep only why it is not host:port.
		if ae := (*net.AddrError)(nil); errors.As(err, &ae) {
			return errors.New(ae.Err)
		}
		return err
	}
	if host == "" {
		return errors.New("missing host")
	}
	if err := checkPort(port); err != nil {
		return err
	}
	return checkHost(host)
}

func checkPort(port string) error {
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("port %q is not a number from 1 to 65535", port)
	}
	return nil
}

// checkHost accepts an IP address (IPv6 without brackets) or a host name.
func checkHost(host string) error {
	if _, err := netip.ParseAddr(host); err == nil {
		return nil
	}
	for _, r := range host {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '.' && r != '_' {
			return fmt.Errorf("host %q is not an IP address or a host name", host)
		}
	}
	return nil
}

// remotesNeedModel refuses a harness registered by address when -model is unset: this host answers
// a remote harness's model calls, and without -model the built-in echo stub would answer them and be
// journaled as if a model had.
func remotesNeedModel(model string, remotes remoteHarnesses) error {
	if len(remotes) > 0 && model == "" {
		return fmt.Errorf("-harness %s requires -model: this host answers a remote harness's model calls, and without -model they would go to the built-in echo stub", remotes)
	}
	return nil
}

// reservedHarnesses names every harness this binary can serve itself, whatever its flags. All of them
// are reserved in the journal, chat included on a host started without -model, so a registered
// harness cannot take "chat" and then collide with it the next time the host starts with -model.
var reservedHarnesses = []string{"chat", "echo"}

// builtinHarnesses names the harnesses this binary serves itself: echo always, and chat with -model.
func builtinHarnesses(model string) []string {
	if model == "" {
		return []string{"echo"}
	}
	return slices.Clone(reservedHarnesses)
}

// checkRemotes runs the -harness checks that need no side effect, so main can refuse a misconfigured
// start before it opens the journal. harnessRegistry repeats them as a backstop.
func checkRemotes(model string, remotes remoteHarnesses) error {
	if err := remotesNeedModel(model, remotes); err != nil {
		return err
	}
	builtin := builtinHarnesses(model)
	// Sorted, so a startup with two colliding names reports the same one every time.
	for _, name := range remotes.names() {
		if slices.Contains(builtin, name) {
			return errNameTaken(name)
		}
	}
	return nil
}

func errNameTaken(name string) error {
	return fmt.Errorf("harness %q is already served by this binary; pick another name", name)
}

// harnessRegistry owns the local backends as a group so every startup error and shutdown closes
// all of them. Echo remains the default; chat requires an explicitly configured model.
func harnessRegistry(model string, remotes remoteHarnesses, modelFn controller.ModelFunc, streamFn controller.StreamFunc, logger *slog.Logger) (*placement.Registry, func(), error) {
	echo := local.New(echoagent.Harness{}, local.WithLogger(logger))
	backends := []*local.Backend{echo}
	var remoteBackends []*remote.Backend
	closeBackends := func() {
		for _, backend := range backends {
			_ = backend.Close()
		}
		for _, backend := range remoteBackends {
			_ = backend.Close()
		}
	}
	opts := []placement.Option{
		placement.WithLogger(logger),
		placement.WithStreamingModel(streamFn),
	}
	placers := map[string]*placement.Placer{
		"echo": placement.New(echo, modelFn, opts...),
	}
	if model != "" {
		chat := local.New(chatagent.Harness{Model: model}, local.WithLogger(logger))
		backends = append(backends, chat)
		placers["chat"] = placement.New(chat, modelFn, opts...)
	}
	if err := remotesNeedModel(model, remotes); err != nil {
		closeBackends()
		return nil, nil, err
	}
	// Sorted, so a startup with two colliding names reports the same one every time.
	for _, name := range remotes.names() {
		addr := remotes[name]
		if _, taken := placers[name]; taken {
			closeBackends()
			return nil, nil, errNameTaken(name)
		}
		backend := remote.New(addr, remote.WithLogger(logger))
		remoteBackends = append(remoteBackends, backend)
		placers[name] = placement.New(backend, modelFn, opts...)
	}
	// The -harness names are static entries, so ReserveStaticHarnessNames reserves them along with
	// the built-in ones.
	registry, err := placement.NewRegistry("echo", placers, placement.WithReservedNames(reservedHarnesses...))
	if err != nil {
		closeBackends()
		return nil, nil, fmt.Errorf("build harness registry: %w", err)
	}
	return registry, closeBackends, nil
}

// sessionService builds the Sessions service, after reserving this host's harness names in the
// journal. This host does not serve the registry, but a Go host that does may share the journal,
// and its static names can differ: a registration named "echo" would otherwise mean a second
// harness under the built-in name. Reserving refuses a journal that already holds such a
// registration, and makes the other host refuse one made later.
func sessionService(store *sqlitelog.Store, registry *placement.Registry, logger *slog.Logger, project string) (*session.Service, error) {
	if err := session.ReserveStaticHarnessNames(store, registry); err != nil {
		return nil, fmt.Errorf("journal: %w", err)
	}
	return session.NewService(store, registry, session.WithLogger(logger), session.WithDefaultProject(project)), nil
}

// modelFunc selects the model the host mediates. Empty -model keeps the built-in echo model so the
// quickstart runs with no key; otherwise it builds an OpenAI-compatible client.
//
// The credential is read from MODEL_API_KEY (or OPENAI_API_KEY) rather than taken as a flag,
// because a flag would put it in the process list and in shell history. An unset key sends no
// credential header at all, which is what a local endpoint or a gateway that injects its own
// expects.
//
// -model-path and -model-auth-header exist because endpoints differ in envelope while accepting
// the same request body: some scope the model into the path or pin an API version, and some
// authenticate with a header other than Authorization. Without these, reaching one of those would
// mean recompiling the server for a difference that is pure configuration.
func modelFunc(model, baseURL, path, authHeader string) (controller.ModelFunc, controller.StreamFunc, string, error) {
	if model == "" {
		// The built-in model answers instantly, so there is nothing to stream.
		return echoagent.Model, nil, "echo (built-in)", nil
	}
	client, err := modelconfig.New(modelconfig.Config{
		Model:      model,
		BaseURL:    baseURL,
		Path:       path,
		AuthHeader: authHeader,
		APIKey:     modelAPIKey(),
	})
	if err != nil {
		return nil, nil, "", fmt.Errorf("configure model: %w", err)
	}
	return client.Model, client.StreamModel, model + " @ " + baseURL + path, nil
}

// modelAPIKey reads the credential, preferring the endpoint-neutral name. OPENAI_API_KEY is
// accepted too, since that is the variable most tooling already sets.
func modelAPIKey() string {
	if k := os.Getenv("MODEL_API_KEY"); k != "" {
		return k
	}
	return os.Getenv("OPENAI_API_KEY")
}
