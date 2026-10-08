//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly || illumos

package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"syscall"
	"time"
)

// listenUnix listens on the unix socket path as its sole owner, first removing a socket file that a
// previous harnessnode left behind when it died without unlinking it, so a restart can listen on the
// same address again. The caller holds the returned lock file open for as long as it serves.
//
// A failed dial alone cannot prove a socket is stale: a live listener whose accept backlog is full
// refuses connections too. Ownership is instead an exclusive flock on path+".lock", which the kernel
// releases however the owner exits, and the lock file records which socket file its owner bound.
// So listenUnix:
//   - fails if another harnessnode holds the lock, however busy its listener is;
//   - removes an existing socket only when it is the one the lock file records and a dial gets
//     ECONNREFUSED. A socket some other process created, or any other dial result, is an error and
//     the socket is left in place;
//   - never removes anything that is not a socket.
//
// The lock file is left in place on exit; removing it would let two starts lock different files.
func listenUnix(path string) (*net.UnixListener, *os.File, error) {
	lockPath := path + ".lock"
	lock, err := openLockFile(lockPath)
	if err != nil {
		return nil, nil, err
	}
	lis, err := func() (*net.UnixListener, error) {
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			if errors.Is(err, syscall.EWOULDBLOCK) {
				return nil, fmt.Errorf("%s is in use by another harnessnode", path)
			}
			return nil, fmt.Errorf("lock %s: %w", lockPath, err)
		}
		recorded, err := io.ReadAll(lock)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", lockPath, err)
		}
		if err := removeStaleSocket(path, string(recorded)); err != nil {
			return nil, err
		}
		lis, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			return nil, err
		}
		if err := recordSocket(lock, path); err != nil {
			_ = lis.Close()
			return nil, fmt.Errorf("record %s in %s: %w", path, lockPath, err)
		}
		return lis, nil
	}()
	if err != nil {
		_ = lock.Close()
		return nil, nil, err
	}
	return lis, lock, nil
}

// openLockFile opens the lock file without following a symlink and refuses anything but a regular
// file this user owns with no other name. listenUnix truncates and rewrites the lock file, so whoever
// can write to the socket's directory must not be able to point it at another file: a symlink would
// redirect the write to its target, and a hard link would share the target's contents.
func openLockFile(lockPath string) (*os.File, error) {
	lock, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, fmt.Errorf("lock %s: is a symlink; remove it", lockPath)
		}
		return nil, fmt.Errorf("lock %s: %w", lockPath, err)
	}
	if err := checkLockFile(lock); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("lock %s: %w; remove it", lockPath, err)
	}
	return lock, nil
}

func checkLockFile(lock *os.File) error {
	fi, err := lock.Stat()
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("is not a regular file (%s)", fi.Mode().Type())
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("cannot read its owner")
	}
	if uid := os.Geteuid(); int(st.Uid) != uid {
		return fmt.Errorf("is owned by uid %d, not %d", st.Uid, uid)
	}
	if uint64(st.Nlink) != 1 {
		return fmt.Errorf("has %d links, want 1", st.Nlink)
	}
	return nil
}

// socketID identifies the file at path, so a later start can tell the socket a harnessnode bound
// from one another process bound at the same path since.
func socketID(fi os.FileInfo) string {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%v:%v", st.Dev, st.Ino)
}

func recordSocket(lock *os.File, path string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if err := lock.Truncate(0); err != nil {
		return err
	}
	if _, err := lock.WriteAt([]byte(socketID(fi)), 0); err != nil {
		return err
	}
	return lock.Sync()
}

// removeStaleSocket removes the socket at path if it is the one a dead harnessnode recorded in the
// lock file the caller now holds.
func removeStaleSocket(path, recorded string) error {
	fi, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if fi.Mode()&fs.ModeSocket == 0 {
		return fmt.Errorf("%s exists and is not a socket", path)
	}
	if id := socketID(fi); id == "" || id != recorded {
		return fmt.Errorf("%s exists and was not left by a harnessnode; remove it if nothing serves on it", path)
	}
	c, err := net.DialTimeout("unix", path, time.Second)
	if err == nil {
		_ = c.Close()
		return fmt.Errorf("%s is in use by another process", path)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("%s exists and cannot be confirmed stale: %w", path, err)
	}
	return os.Remove(path)
}
