//go:build linux

// Package controllerownership holds local Controller execution ownership for
// the process lifetime. This is not proof that external Docker requests ended.
package controllerownership

import (
	"context"
	"errors"
	"os"
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const directory = "/run/groundplane-execution"

type Lease struct {
	mu     sync.Mutex
	fd     int
	opened bool
	held   bool
	closed bool
}

// Acquire never waits for or steals ownership. The pinned private directory is
// deliberately retained on Close: unlinking a locked inode could permit another
// process to acquire a different inode at the same name.
func Acquire(ctx context.Context) (*Lease, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "controller ownership requires a context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	fd, err := unix.Openat2(unix.AT_FDCWD, directory, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	lease := &Lease{fd: fd, opened: true}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, errs.WrapJoined(errs.KindInternal, err, lease.Close())
	}
	if stat.Uid != uint32(os.Geteuid()) || stat.Mode&0o777 != 0o700 {
		return nil, errs.WrapJoined(errs.KindStateConflict, errs.New(errs.KindStateConflict, "controller ownership directory must be private and owned by the Controller account"), lease.Close())
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeErr := lease.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, errs.WrapJoined(errs.KindStateConflict, errs.New(errs.KindStateConflict, "another Controller owns local execution"), closeErr)
		}
		return nil, errs.WrapJoined(errs.KindInternal, err, closeErr)
	}
	lease.held = true
	if err := ctx.Err(); err != nil {
		return nil, errs.WrapJoined(errs.KindInternal, err, lease.Close())
	}
	return lease, nil
}

// Check prevents zero-value or released ownership from authorizing startup.
func (lease *Lease) Check() error {
	if lease == nil {
		return errs.New(errs.KindStateConflict, "controller process ownership is required")
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed || !lease.opened || !lease.held {
		return errs.New(errs.KindStateConflict, "controller process ownership is closed")
	}
	return nil
}

func (lease *Lease) Close() error {
	if lease == nil {
		return nil
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return nil
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
