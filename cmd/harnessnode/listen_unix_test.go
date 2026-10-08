//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly || illumos

package main

import (
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// A harness killed without cleanup leaves its socket file behind. Restarting on the same address
// must reclaim it, but never take over a socket another harness is still serving on, however busy,
// never remove a socket no harnessnode created, and never delete a file that is not a socket.
func TestClaimUnixSocket(t *testing.T) {
	dir, err := os.MkdirTemp("", "hn") // short path: unix socket paths are length-limited
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	// serve is a harnessnode serving on path; closing it is a graceful exit.
	serve := func(t *testing.T, path string) (*net.UnixListener, *os.File) {
		t.Helper()
		lis, lock, err := listenUnix(path)
		if err != nil {
			t.Fatalf("listen %s: %v", path, err)
		}
		return lis, lock
	}
	// crashed leaves what a SIGKILL leaves: a socket file nobody accepts on, and a lock file
	// nobody holds.
	crashed := func(t *testing.T, path string) {
		t.Helper()
		lis, lock := serve(t, path)
		lis.SetUnlinkOnClose(false)
		_ = lis.Close()
		_ = lock.Close()
	}
	refused := func(t *testing.T, path string, before os.FileInfo) {
		t.Helper()
		if lis, lock, err := listenUnix(path); err == nil {
			_ = lis.Close()
			_ = lock.Close()
			t.Fatal("took over a socket it must not")
		}
		after, err := os.Lstat(path)
		if err != nil {
			t.Fatalf("socket was removed: %v", err)
		}
		if !os.SameFile(before, after) {
			t.Fatal("socket was replaced")
		}
	}

	t.Run("missing", func(t *testing.T) {
		lis, lock := serve(t, filepath.Join(dir, "none.sock"))
		_ = lis.Close()
		_ = lock.Close()
	})

	t.Run("stale", func(t *testing.T) {
		path := filepath.Join(dir, "stale.sock")
		crashed(t, path)
		lis, lock := serve(t, path)
		defer lock.Close()
		defer lis.Close()
		// and again after a graceful exit, which unlinks the socket but keeps the lock file.
		_ = lis.Close()
		_ = lock.Close()
		lis, lock = serve(t, path)
		_ = lis.Close()
		_ = lock.Close()
	})

	// A live listener whose accept backlog is full refuses connections exactly as a dead one does,
	// so a failed dial must not be read as "stale".
	t.Run("live with a full backlog", func(t *testing.T) {
		path := filepath.Join(dir, "busy.sock")
		lis, lock := serve(t, path)
		defer lock.Close()
		defer lis.Close()
		setBacklog(t, lis, 1)
		fillBacklog(t, path)
		before, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		refused(t, path, before)
	})

	t.Run("concurrent restarts", func(t *testing.T) {
		path := filepath.Join(dir, "race.sock")
		crashed(t, path)
		const n = 8
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			serving int
			errs    []error
		)
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				lis, lock, err := listenUnix(path)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs = append(errs, err)
					return
				}
				serving++
				t.Cleanup(func() { _ = lis.Close(); _ = lock.Close() })
			}()
		}
		wg.Wait()
		if serving != 1 {
			t.Fatalf("%d of %d concurrent restarts are serving the address, want exactly 1; errors: %v", serving, n, errs)
		}
		if c, err := net.Dial("unix", path); err != nil {
			t.Fatalf("the winner is not reachable at the address: %v", err)
		} else {
			_ = c.Close()
		}
	})

	// A socket no harnessnode created is not ours to remove, whether or not anything serves on it.
	t.Run("foreign", func(t *testing.T) {
		path := filepath.Join(dir, "foreign.sock")
		lis := listenWithBacklog(t, path, 1)
		fillBacklog(t, path)
		before, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		refused(t, path, before)
		_ = lis.Close()
		refused(t, path, before)
	})

	// Only ECONNREFUSED marks a socket stale; any other dial error leaves it in place.
	t.Run("dial error other than refused", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root bypasses the socket permission check")
		}
		path := filepath.Join(dir, "denied.sock")
		crashed(t, path)
		if err := os.Chmod(path, 0); err != nil {
			t.Fatal(err)
		}
		before, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		refused(t, path, before)
	})

	// listenUnix rewrites the lock file, so whoever can write the socket's directory must not be able
	// to aim that write at another file through a symlink or a hard link.
	t.Run("lock file is a link", func(t *testing.T) {
		const keep = "IMPORTANT USER DATA\n"
		victim := filepath.Join(dir, "victim")
		if err := os.WriteFile(victim, []byte(keep), 0o600); err != nil {
			t.Fatal(err)
		}
		for _, tt := range []struct {
			name   string
			target string
			link   func(target, lockPath string) error
		}{
			{name: "symlink to an existing file", target: victim, link: os.Symlink},
			{name: "dangling symlink", target: filepath.Join(dir, "absent"), link: os.Symlink},
			{name: "hard link", target: victim, link: os.Link},
		} {
			t.Run(tt.name, func(t *testing.T) {
				path := filepath.Join(dir, "linked.sock")
				t.Cleanup(func() { _ = os.Remove(path + ".lock"); _ = os.Remove(path) })
				if err := tt.link(tt.target, path+".lock"); err != nil {
					t.Fatal(err)
				}
				if lis, lock, err := listenUnix(path); err == nil {
					_ = lis.Close()
					_ = lock.Close()
					t.Fatal("listened with a linked lock file")
				}
				if _, err := os.Lstat(path); err == nil {
					t.Fatal("bound the socket despite refusing the lock file")
				}
				if tt.target == victim {
					if got, err := os.ReadFile(victim); err != nil || string(got) != keep {
						t.Fatalf("link target changed: %q, %v", got, err)
					}
				} else if _, err := os.Lstat(tt.target); err == nil {
					t.Fatal("created the dangling symlink's target")
				}
			})
		}
	})

	t.Run("not a socket", func(t *testing.T) {
		path := filepath.Join(dir, "file")
		if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		if lis, lock, err := listenUnix(path); err == nil {
			_ = lis.Close()
			_ = lock.Close()
			t.Fatal("took over a regular file")
		}
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("regular file was removed: %v", err)
		}
	})
}

// listenWithBacklog is a process other than harnessnode listening on path, with an accept backlog
// of n so a test can fill it with a handful of connections.
func listenWithBacklog(t *testing.T, path string, n int) net.Listener {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Listen(fd, n); err != nil {
		t.Fatal(err)
	}
	lis, err := net.FileListener(f)
	if err != nil {
		t.Fatal(err)
	}
	return lis
}

// setBacklog shrinks a listening socket's accept backlog to n (listen(2) on a listening socket
// updates it), so a test can fill it with a handful of connections.
func setBacklog(t *testing.T, lis *net.UnixListener, n int) {
	t.Helper()
	f, err := lis.File()
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := syscall.Listen(int(f.Fd()), n); err != nil {
		t.Fatal(err)
	}
}

// fillBacklog connects to the listener at path, without it ever accepting, until a connection fails.
func fillBacklog(t *testing.T, path string) {
	t.Helper()
	for range 64 {
		c, err := net.DialTimeout("unix", path, 100*time.Millisecond)
		if err != nil {
			return
		}
		t.Cleanup(func() { _ = c.Close() })
	}
	t.Fatal("accept backlog never filled")
}
