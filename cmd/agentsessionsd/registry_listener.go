package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strconv"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/remote"
	"github.com/aramase/agentsessions/session"
)

// checkRegistryAddress deliberately does not resolve names: the admin API is unauthenticated,
// and DNS results can change between validation and binding. Port zero is useful for test hosts.
func checkRegistryAddress(addr string) (int, error) {
	invalid := func(why string) (int, error) {
		return 0, fmt.Errorf("-registry-addr %q: %s; use a literal loopback IP and port (e.g. 127.0.0.1:8081)", addr, why)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return invalid(err.Error())
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Zone() != "" || !ip.Unmap().IsLoopback() {
		return invalid("host is not a literal loopback IP")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 || port == "" {
		return invalid("port must be a number from 0 to 65535")
	}
	return n, nil
}

// Refuse a shared nonzero port even if one listener uses a wildcard and the OS would
// permit aliasing that port. On some platforms SO_REUSEADDR permits both binds.
func checkListenerPorts(sessionsAddr string, registryPort int) error {
	_, port, err := net.SplitHostPort(sessionsAddr)
	if err != nil {
		return nil
	} // Preserve the Sessions listener's existing validation/bind errors.
	n, err := net.LookupPort("tcp", port)
	if err == nil && n != 0 && n == registryPort {
		return fmt.Errorf("-registry-addr and -addr cannot share TCP port %d", n)
	}
	return nil
}

// registeredPlacer constructs a backend but does not dial it: the harness may be down at
// registration or restart, and Describe is deferred until a session needs it.
func registeredPlacer(model string, modelFn controller.ModelFunc, streamFn controller.StreamFunc, logger *slog.Logger) session.PlacerFactory {
	return func(name string, spec *v1.HarnessSpec) (*placement.Placer, func(), error) {
		switch p := spec.GetPlacement().(type) {
		case *v1.HarnessSpec_Remote:
			if err := checkHarnessAddress(p.Remote.GetAddress()); err != nil {
				return nil, nil, fmt.Errorf("remote harness %q: %w", name, err)
			}
			if model == "" {
				return nil, nil, errors.New("remote registration requires -model: model calls must not fall back to the built-in echo stub")
			}
			backend := remote.New(p.Remote.GetAddress(), remote.WithLogger(logger))
			placer := placement.New(backend, modelFn, placement.WithLogger(logger), placement.WithStreamingModel(streamFn), placement.WithDescriptorID(spec.GetDescriptorId()))
			return placer, func() { _ = backend.Close() }, nil
		case *v1.HarnessSpec_Substrate:
			return nil, nil, errors.New("substrate placement is not supported by agentsessionsd (no substrate backend configured)")
		default:
			return nil, nil, errors.New("placement is required")
		}
	}
}
