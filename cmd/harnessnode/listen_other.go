//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly || illumos)

package main

import (
	"errors"
	"net"
	"os"
)

// listenUnix is unsupported here: claiming a unix socket safely relies on flock and on the owner,
// device and inode a unix stat reports, and syscall.Flock does not exist on every unix (solaris and
// aix lack it). HARNESS_ADDR as host:port still works on this platform.
func listenUnix(string) (*net.UnixListener, *os.File, error) {
	return nil, nil, errors.New("unix:// addresses are not supported on this platform; use host:port")
}
