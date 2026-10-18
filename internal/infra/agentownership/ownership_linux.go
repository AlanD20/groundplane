//go:build linux

// Package agentownership excludes overlapping Agent processes using the shared
// host state mount. Helper containers and accepted Docker mutations require
// separate reconciliation after the previous process releases ownership.
package agentownership

import (
	"context"
	"errors"
	"os"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const directory = agentprotocol.StatePath + "/execution-owner"

type Lease struct {
	mu     sync.Mutex
	fd     int
	opened bool
	held   bool
	closed bool
	active map[string]workexecution.Attempt
}

// Acquire is nonblocking. Never unlink the directory, including on shutdown:
// replacing a locked inode would allow two Agents to own the same host.
func Acquire(ctx context.Context) (*Lease, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "Agent process ownership requires a context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	fd, err := openDirectory()
	if err != nil {
		return nil, err
	}
	lease := &Lease{fd: fd, opened: true, active: make(map[string]workexecution.Attempt)}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, errs.WrapJoined(errs.KindInternal, err, lease.Close())
	}
	if stat.Uid != uint32(os.Geteuid()) || stat.Mode&0o7777 != 0o700 {
		return nil, errs.WrapJoined(errs.KindStateConflict, errs.New(errs.KindStateConflict, "Agent execution ownership directory must be private and owned by the Agent account"), lease.Close())
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeErr := lease.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, errs.WrapJoined(errs.KindStateConflict, errs.New(errs.KindStateConflict, "another Agent process still owns this host's execution"), closeErr)
		}
		return nil, errs.WrapJoined(errs.KindInternal, err, closeErr)
	}
	lease.held = true
	if err := ctx.Err(); err != nil {
		return nil, errs.WrapJoined(errs.KindInternal, err, lease.Close())
	}
	if err := lease.Check(); err != nil {
		return nil, errs.WrapJoined(errs.KindStateConflict, err, lease.Close())
	}
	return lease, nil
}

func openDirectory() (int, error) {
	fd, err := unix.Openat2(unix.AT_FDCWD, directory, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return -1, errs.Wrap(errs.KindInternal, err)
	}
	return fd, nil
}

// Check also detects replacement of the shared path. Holding an unlinked inode
// must not authorize new execution against a different Agent's state mount.
func (lease *Lease) Check() error {
	if lease == nil {
		return errs.New(errs.KindStateConflict, "Agent process ownership is required")
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed || !lease.opened || !lease.held {
		return errs.New(errs.KindStateConflict, "Agent process ownership is closed")
	}
	fd, err := openDirectory()
	if err != nil {
		return err
	}
	var held, current unix.Stat_t
	statErr := errors.Join(unix.Fstat(lease.fd, &held), unix.Fstat(fd, &current))
	if err := errors.Join(statErr, unix.Close(fd)); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if held.Dev != current.Dev || held.Ino != current.Ino || current.Uid != uint32(os.Geteuid()) || current.Mode&0o7777 != 0o700 {
		return errs.New(errs.KindStateConflict, "Agent execution ownership directory changed")
	}
	return nil
}

// Close is called only after local workers are joined and runtime clients close.
func (lease *Lease) Close() error {
	if lease == nil {
		return nil
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return nil
	}
	if len(lease.active) != 0 {
		return errs.New(errs.KindStateConflict, "Agent process ownership cannot close while work is executing")
	}
	lease.closed = true
	if !lease.opened {
		return nil
	}
	if err := unix.Close(lease.fd); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
