package session

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/canon"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/sqlitelog"
)

// PlacerFactory builds the Placer for a registered harness, and a release func that frees whatever
// it built (nil if nothing). It is where the host decides which placements it can serve and checks
// a placement's address forms: an error is reported as FailedPrecondition and nothing is stored. It
// must not dial or provision anything, because it also runs at startup for every stored
// registration, and a harness that is down must not keep the host from starting.
//
// The Placer must be built with placement.WithDescriptorID(spec.GetDescriptorId()), so that every
// Exec, Resume and Fork refuses a harness that reports another descriptor id. HarnessRegistry
// checks this and refuses a Placer that does not expect exactly that id, so a factory that drops the
// option fails closed instead of running turns on whatever answers.
//
// The Placer must be new: not used, and not in any Registry. placement.Registry.Add wires it to the
// registry's shared session guard, so its sessions cannot overlap calls through a static harness.
//
// For a RemotePlacement, runtime/remote is the backend that dials a harness already running at an
// address; agentsessionsd's -harness flag uses it the same way.
type PlacerFactory func(name string, spec *v1.HarnessSpec) (p *placement.Placer, release func(), err error)

// HarnessRegistry implements v1.HarnessRegistryServer over the store and placement registry the
// Sessions service routes through.
//
// It is an admin surface: whoever can call RegisterHarness decides where the host sends session
// history and whose credential pays for model calls. agentsessionsd does not serve it. Wire it
// in-process, or on a listener only an operator can reach.
type HarnessRegistry struct {
	v1.UnimplementedHarnessRegistryServer
	store    *sqlitelog.Store
	registry *placement.Registry
	factory  PlacerFactory

	// mu serializes RegisterHarness, so building a Placer, storing the row and adding the Placer
	// happen as one step per name within this process.
	mu       sync.Mutex
	releases []func()
}

// NewHarnessRegistry builds the service and makes every stored registration resolvable, retired
// ones included, because existing sessions on a retired harness still run. It first reserves the
// registry's static and reserved names in the store, as ReserveStaticHarnessNames does, so a stored
// registration under one of them refuses startup rather than letting one name mean two harnesses.
//
// Registrations are loaded once, here. A registration another process makes in a shared database
// is served by this one after it restarts.
func NewHarnessRegistry(store *sqlitelog.Store, registry *placement.Registry, factory PlacerFactory) (*HarnessRegistry, error) {
	if store == nil || registry == nil || factory == nil {
		return nil, errors.New("session: NewHarnessRegistry needs a store, a registry and a factory")
	}
	if err := ReserveStaticHarnessNames(store, registry); err != nil {
		return nil, err
	}
	h := &HarnessRegistry{store: store, registry: registry, factory: factory}
	if err := forEachRegistration(store, h.load); err != nil {
		h.Close()
		return nil, err
	}
	return h, nil
}

// ReserveStaticHarnessNames reserves, in the store, every name the registry holds for a static
// harness, so that no host sharing the store can register one of them, now or later. It fails if
// one of them already has a registration, active or retired. Every host that routes sessions
// through a store other hosts may register harnesses in calls it before serving: otherwise a
// registration made by a host whose static names differ would mean a second harness under one of
// this host's names. NewHarnessRegistry calls it.
//
// Service also refuses, when it creates or routes a session, a static name that has a
// registration row, so a host that skips this call fails closed instead of mixing the two. The
// reservation is what lets the registering host refuse the name up front instead.
func ReserveStaticHarnessNames(store *sqlitelog.Store, registry *placement.Registry) error {
	if store == nil || registry == nil {
		return errors.New("session: ReserveStaticHarnessNames needs a store and a registry")
	}
	if err := store.ReserveHarnessNames(registry.ReservedNames()); err != nil {
		return fmt.Errorf("session: %w", err)
	}
	return nil
}

// forEachRegistration calls fn for every stored registration, retired ones included, in name order.
func forEachRegistration(store *sqlitelog.Store, fn func(sqlitelog.HarnessRecord) error) error {
	after := ""
	for {
		recs, err := store.ListHarnesses(sqlitelog.HarnessListOptions{After: after, Limit: sqlitelog.MaxPageSize, IncludeRetired: true})
		if err != nil {
			return err
		}
		for _, rec := range recs {
			if err := fn(rec); err != nil {
				return err
			}
		}
		if len(recs) < sqlitelog.MaxPageSize {
			return nil
		}
		after = recs[len(recs)-1].Name
	}
}

func (h *HarnessRegistry) load(rec sqlitelog.HarnessRecord) error {
	spec, err := decodeSpec(rec.Spec)
	if err != nil {
		return fmt.Errorf("session: harness %q: stored spec: %w", rec.Name, err)
	}
	p, release, err := h.factory(rec.Name, spec)
	if err != nil {
		return fmt.Errorf("session: harness %q: %w", rec.Name, err)
	}
	h.keep(release)
	if err := checkPlacer(p, spec); err != nil {
		return fmt.Errorf("session: harness %q: %w", rec.Name, err)
	}
	return h.registry.Add(rec.Name, p)
}

// checkPlacer refuses what a PlacerFactory returned when it cannot serve the spec as registered: no
// Placer at all, or one that does not check the descriptor id the spec expects. Both are host bugs,
// and running the harness anyway would send session history to a harness nobody registered.
func checkPlacer(p *placement.Placer, spec *v1.HarnessSpec) error {
	if p == nil {
		return errors.New("the placer factory returned no placer")
	}
	if got, want := p.DescriptorID(), spec.GetDescriptorId(); got != want {
		return fmt.Errorf("the placer factory returned a placer that expects descriptor id %q, the spec expects %q; build it with placement.WithDescriptorID(spec.GetDescriptorId())", got, want)
	}
	return nil
}

func (h *HarnessRegistry) keep(release func()) {
	if release != nil {
		h.releases = append(h.releases, release)
	}
}

// Close releases every Placer the factory built. Call it after the servers that route through the
// registry have stopped.
func (h *HarnessRegistry) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, release := range h.releases {
		release()
	}
	h.releases = nil
}

// harnessName is an RFC 1123 DNS label.
var harnessName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// Bounds on what one registration stores. They are far above any real spec or reason, and exist so
// an admin call cannot grow the database without limit.
const (
	maxSpecBytes     = 4096
	maxRetireReason  = 1024
	observeTimeout   = 10 * time.Second
	harnessUIDPrefix = "harness-"
)

// specDigest returns the spec's canonical bytes and "sha256:<hex>" over them. The canonical form is
// the journal's (package canon), so any implementation can recompute the digest.
func specDigest(spec *v1.HarnessSpec) (canonical []byte, digest string, err error) {
	c, err := canon.Proto(spec)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(c)
	return c, "sha256:" + hex.EncodeToString(sum[:]), nil
}

func decodeSpec(s string) (*v1.HarnessSpec, error) {
	spec := &v1.HarnessSpec{}
	if err := protojson.Unmarshal([]byte(s), spec); err != nil {
		return nil, err
	}
	return spec, nil
}

func validateSpec(spec *v1.HarnessSpec) error {
	if spec == nil {
		return errors.New("spec is required")
	}
	// A field this build does not know would be dropped from the canonical form, so the digest and
	// the stored spec would not say what the caller asked for. An enum number this build does not
	// define is kept, but it declares something, such as a resumability, this host cannot honor.
	if err := checkKnown(spec.ProtoReflect()); err != nil {
		return err
	}
	switch p := spec.GetPlacement().(type) {
	case *v1.HarnessSpec_Remote:
		if p.Remote.GetAddress() == "" {
			return errors.New("remote.address is required")
		}
	case *v1.HarnessSpec_Substrate:
		if p.Substrate.GetAtespace() == "" || p.Substrate.GetTemplate() == "" {
			return errors.New("substrate.atespace and substrate.template are required")
		}
	default:
		return errors.New("placement is required")
	}
	if spec.GetCapabilities().GetResumability() == v1.Resumability_RESUMABILITY_UNSPECIFIED {
		return errors.New("capabilities.resumability is required")
	}
	return nil
}

// checkKnown reports the first unknown field, or enum value this build does not define, in m or in
// any message it holds.
func checkKnown(m protoreflect.Message) error {
	if len(m.GetUnknown()) > 0 {
		return fmt.Errorf("%s has fields this host does not understand", m.Descriptor().Name())
	}
	var err error
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
			// HarnessSpec has no map fields; refuse one rather than skip what it holds.
			err = fmt.Errorf("%s: map fields are not supported", fd.Name())
		case fd.IsList():
			for i := 0; i < v.List().Len() && err == nil; i++ {
				err = checkValue(fd, v.List().Get(i))
			}
		default:
			err = checkValue(fd, v)
		}
		return err == nil
	})
	return err
}

func checkValue(fd protoreflect.FieldDescriptor, v protoreflect.Value) error {
	switch {
	case fd.Message() != nil:
		return checkKnown(v.Message())
	case fd.Enum() != nil:
		if fd.Enum().Values().ByNumber(v.Enum()) == nil {
			return fmt.Errorf("%s: %d is not a value this host understands", fd.Name(), v.Enum())
		}
	}
	return nil
}

func newHarnessUID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return harnessUIDPrefix + hex.EncodeToString(b[:])
}

func validName(name string) error {
	if !harnessName.MatchString(name) {
		return status.Errorf(codes.InvalidArgument, "name %q is not a DNS label (lowercase alphanumerics and '-', at most 63)", name)
	}
	return nil
}

// RegisterHarness stores a registration and makes it resolvable, or recognizes a repeat of one.
func (h *HarnessRegistry) RegisterHarness(ctx context.Context, req *v1.RegisterHarnessRequest) (*v1.RegisterHarnessResponse, error) {
	name, spec := req.GetName(), req.GetSpec()
	if err := validName(name); err != nil {
		return nil, err
	}
	if h.registry.Reserved(name) {
		return nil, status.Errorf(codes.AlreadyExists, "harness %q is a static harness name on this host and cannot be registered", name)
	}
	if err := validateSpec(spec); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "spec: %v", err)
	}
	canonical, digest, err := specDigest(spec)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "spec: %v", err)
	}
	if len(canonical) > maxSpecBytes {
		return nil, status.Errorf(codes.InvalidArgument, "spec is %d bytes in canonical form; the limit is %d", len(canonical), maxSpecBytes)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	// Fail a conflicting spec before building anything for it. The store checks again under its
	// write lock, which is what holds against another process.
	if rec, err := h.store.Harness(name); err == nil && rec.SpecDigest != digest {
		return nil, status.Errorf(codes.AlreadyExists, "harness %q holds spec %s, request is %s; registrations are immutable, use a new name", name, rec.SpecDigest, digest)
	} else if err != nil && !errors.Is(err, sqlitelog.ErrHarnessNotFound) {
		return nil, status.Errorf(codes.Internal, "register: %v", err)
	}
	// Build the Placer before anything is stored, so a placement this host cannot serve is refused
	// with nothing written. It is added only after the row commits: a Placer resolvable under a
	// name whose row then failed to store would run sessions on a spec nobody registered.
	var placer *placement.Placer
	var release func()
	if !h.registry.Has(name) {
		if placer, release, err = h.factory(name, spec); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "harness %q: %v", name, err)
		}
		if err := checkPlacer(placer, spec); err != nil {
			// Storing the row would report CREATED for a name no session can use, or one whose
			// turns skip the descriptor check, and the row is immutable.
			if release != nil {
				release()
			}
			return nil, status.Errorf(codes.Internal, "harness %q: %v", name, err)
		}
	}
	rec, result, err := h.store.RegisterHarness(sqlitelog.HarnessRecord{
		Name: name, UID: newHarnessUID(), Spec: string(canonical), SpecDigest: digest,
	})
	if err != nil {
		if release != nil {
			release()
		}
		if errors.Is(err, sqlitelog.ErrHarnessSpecConflict) {
			return nil, status.Errorf(codes.AlreadyExists, "%v; registrations are immutable, use a new name", err)
		}
		if errors.Is(err, sqlitelog.ErrHarnessNameReserved) {
			return nil, status.Errorf(codes.AlreadyExists, "%v by a host sharing this journal and cannot be registered", err)
		}
		return nil, status.Errorf(codes.Internal, "register: %v", err)
	}
	if placer != nil {
		h.keep(release)
		if err := h.registry.Add(name, placer); err != nil {
			return nil, status.Errorf(codes.Internal, "register: %v", err)
		}
	}
	out, err := registrationProto(rec)
	if err != nil {
		return nil, err
	}
	return &v1.RegisterHarnessResponse{Harness: out, Outcome: registerOutcome(result)}, nil
}

func registerOutcome(r sqlitelog.RegisterResult) v1.RegisterOutcome {
	switch r {
	case sqlitelog.HarnessCreated:
		return v1.RegisterOutcome_REGISTER_OUTCOME_CREATED
	case sqlitelog.HarnessUnchanged:
		return v1.RegisterOutcome_REGISTER_OUTCOME_UNCHANGED
	case sqlitelog.HarnessReactivated:
		return v1.RegisterOutcome_REGISTER_OUTCOME_REACTIVATED
	}
	return v1.RegisterOutcome_REGISTER_OUTCOME_UNSPECIFIED
}

// GetHarness returns one static or registered harness, optionally with what it reports right now.
func (h *HarnessRegistry) GetHarness(ctx context.Context, req *v1.GetHarnessRequest) (*v1.HarnessRegistration, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	var out *v1.HarnessRegistration
	if h.registry.Static(name) {
		out = staticProto(name)
	} else {
		rec, err := h.store.Harness(name)
		if errors.Is(err, sqlitelog.ErrHarnessNotFound) {
			return nil, status.Errorf(codes.NotFound, "harness %q not found", name)
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get: %v", err)
		}
		if out, err = registrationProto(rec); err != nil {
			return nil, err
		}
	}
	if req.GetObserve() {
		h.observe(ctx, out)
	}
	return out, nil
}

// observe fills observed or observe_error. A failed Describe is reported, not returned: the
// registration exists either way, and telling "registered but unreachable" from "not registered" is
// the point of asking.
func (h *HarnessRegistry) observe(ctx context.Context, out *v1.HarnessRegistration) {
	name := out.GetMetadata().GetName()
	if !h.registry.Has(name) {
		out.ObserveError = "harness is not loaded on this host"
		return
	}
	p, err := h.registry.For(name)
	if err != nil {
		out.ObserveError = err.Error()
		return
	}
	ctx, cancel := context.WithTimeout(ctx, observeTimeout)
	defer cancel()
	desc, err := p.Describe(ctx)
	if err != nil {
		out.ObserveError = fmt.Sprintf("describe: %v", err)
		return
	}
	out.Observed = harnesswire.DescriptorToProto(desc)
}

// RetireHarness refuses new sessions on a registered harness. Existing sessions keep running.
func (h *HarnessRegistry) RetireHarness(ctx context.Context, req *v1.RetireHarnessRequest) (*v1.HarnessRegistration, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if h.registry.Static(name) {
		return nil, status.Errorf(codes.FailedPrecondition, "harness %q is a static harness; it is retired by changing the host's configuration", name)
	}
	if len(req.GetReason()) > maxRetireReason {
		return nil, status.Errorf(codes.InvalidArgument, "reason is %d bytes; the limit is %d", len(req.GetReason()), maxRetireReason)
	}
	rec, err := h.store.RetireHarness(name, req.GetReason())
	if errors.Is(err, sqlitelog.ErrHarnessNotFound) {
		return nil, status.Errorf(codes.NotFound, "harness %q not found", name)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "retire: %v", err)
	}
	return registrationProto(rec)
}

// ListHarnesses lists static and registered harnesses together in name order. Paging is keyset on
// the name, so a registration made while a caller pages neither skips nor repeats an entry.
func (h *HarnessRegistry) ListHarnesses(ctx context.Context, req *v1.ListHarnessesRequest) (*v1.ListHarnessesResponse, error) {
	size := int(req.GetPageSize())
	switch {
	case size < 0:
		return nil, status.Error(codes.InvalidArgument, "page_size must not be negative")
	case size == 0:
		size = sqlitelog.DefaultPageSize
	case size > sqlitelog.MaxPageSize:
		size = sqlitelog.MaxPageSize
	}
	after, err := decodeHarnessPageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "page_token: %v", err)
	}
	// One extra row says whether another page exists.
	recs, err := h.store.ListHarnesses(sqlitelog.HarnessListOptions{After: after, Limit: size + 1, IncludeRetired: req.GetIncludeRetired()})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	var static []string
	for _, name := range h.registry.StaticNames() {
		if name > after {
			static = append(static, name)
		}
	}
	out := &v1.ListHarnessesResponse{}
	for len(out.Harnesses) <= size && (len(static) > 0 || len(recs) > 0) {
		if len(recs) == 0 || (len(static) > 0 && static[0] < recs[0].Name) {
			out.Harnesses = append(out.Harnesses, staticProto(static[0]))
			static = static[1:]
			continue
		}
		r, err := registrationProto(recs[0])
		if err != nil {
			return nil, err
		}
		out.Harnesses = append(out.Harnesses, r)
		recs = recs[1:]
	}
	if len(out.Harnesses) > size {
		out.Harnesses = out.Harnesses[:size]
		out.NextPageToken = encodeHarnessPageToken(out.Harnesses[size-1].GetMetadata().GetName())
	}
	return out, nil
}

func encodeHarnessPageToken(after string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(after))
}

func decodeHarnessPageToken(token string) (string, error) {
	if token == "" {
		return "", nil
	}
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(b) == 0 {
		return "", errors.New("not a token this server issued")
	}
	return string(b), nil
}

func staticProto(name string) *v1.HarnessRegistration {
	return &v1.HarnessRegistration{
		Metadata: &v1.ResourceMetadata{Name: name},
		State:    v1.HarnessState_HARNESS_STATE_ACTIVE,
		Source:   v1.HarnessSource_HARNESS_SOURCE_STATIC,
	}
}

func registrationProto(rec sqlitelog.HarnessRecord) (*v1.HarnessRegistration, error) {
	spec, err := decodeSpec(rec.Spec)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "harness %q: stored spec: %v", rec.Name, err)
	}
	out := &v1.HarnessRegistration{
		Metadata: &v1.ResourceMetadata{
			Name:       rec.Name,
			Uid:        rec.UID,
			CreateTime: timestamppb.New(rec.CreatedAt),
			UpdateTime: timestamppb.New(rec.UpdatedAt),
		},
		Spec:       spec,
		SpecDigest: rec.SpecDigest,
		State:      v1.HarnessState_HARNESS_STATE_ACTIVE,
		Source:     v1.HarnessSource_HARNESS_SOURCE_REGISTERED,
	}
	if rec.State == sqlitelog.HarnessRetired {
		out.State = v1.HarnessState_HARNESS_STATE_RETIRED
		out.RetireTime = timestamppb.New(rec.RetiredAt)
		out.RetireReason = rec.RetireReason
	}
	return out, nil
}
