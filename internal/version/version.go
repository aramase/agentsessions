// Package version reports the build's identity.
//
// The values are injected at link time by the release build. A binary built any other way reports
// "dev", which is the honest answer: a development build is not a release and should not claim a
// version number that implies one.
package version

import "runtime/debug"

// Injected via -ldflags by the release build.
var (
	version = "dev"
	commit  = ""
	date    = ""
)

// Info describes the running binary.
type Info struct {
	Version string
	Commit  string
	Date    string
}

// Get reports the build's identity, falling back to what the Go toolchain stamped into the binary.
// A `go install`ed binary carries its module version and VCS revision even with no ldflags, so
// reading them is what keeps those builds identifiable rather than uniformly "dev".
func Get() Info {
	info := Info{Version: version, Commit: commit, Date: date}
	build, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	if info.Version == "dev" && build.Main.Version != "" && build.Main.Version != "(devel)" {
		info.Version = build.Main.Version
	}
	for _, s := range build.Settings {
		switch s.Key {
		case "vcs.revision":
			if info.Commit == "" {
				info.Commit = s.Value
			}
		case "vcs.time":
			if info.Date == "" {
				info.Date = s.Value
			}
		}
	}
	return info
}

// String renders the build for a --version flag.
func (i Info) String() string {
	s := i.Version
	if i.Commit != "" {
		c := i.Commit
		if len(c) > 12 {
			c = c[:12]
		}
		s += " (" + c + ")"
	}
	if i.Date != "" {
		s += " built " + i.Date
	}
	return s
}
