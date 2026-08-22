//go:build linux

// Package environmentroot validates the configured Environment volume root
// through a descriptor-relative Linux boundary before Controller startup.
package environmentroot

import (
	"context"
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const (
	rootOpenFlags = unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW
	rootResolve   = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS
)

type Identity struct {
	Device uint64
	Inode  uint64
}

type descriptorSystem interface {
	OpenRoot(flags int) (int, error)
	OpenComponent(parent int, name string, how *unix.OpenHow) (int, error)
	Stat(fd int, stat *unix.Stat_t) error
	Close(fd int) error
}

type linuxDescriptorSystem struct{}

func (linuxDescriptorSystem) OpenRoot(flags int) (int, error) {
	return unix.Open("/", flags, 0)
}

func (linuxDescriptorSystem) OpenComponent(parent int, name string, how *unix.OpenHow) (int, error) {
	return unix.Openat2(parent, name, how)
}

func (linuxDescriptorSystem) Stat(fd int, stat *unix.Stat_t) error {
	return unix.Fstat(fd, stat)
}

func (linuxDescriptorSystem) Close(fd int) error {
	return unix.Close(fd)
}

// Validate rejects absent, substituted, symlinked, or incorrectly owned roots
// and returns the selected directory's stable device/inode identity.
func Validate(ctx context.Context, root string) (Identity, error) {
	return validate(ctx, root, linuxDescriptorSystem{})
}

func validate(ctx context.Context, root string, system descriptorSystem) (Identity, error) {
	if ctx == nil {
		return Identity{}, errs.New(errs.KindInternal, "environment volume root context is required")
	}
	if err := ctx.Err(); err != nil {
		return Identity{}, err
	}
	if err := environmentpath.ValidateRoot(root); err != nil {
		return Identity{}, err
	}
	if system == nil {
		return Identity{}, errs.New(errs.KindInternal, "environment volume root system is required")
	}
	current, err := system.OpenRoot(rootOpenFlags)
	if err != nil {
		return Identity{}, rootOperationError("open filesystem root", err)
	}
	open := true
	defer func() {
		if open {
			_ = system.Close(current)
		}
	}()
	how := &unix.OpenHow{Flags: rootOpenFlags, Resolve: rootResolve}
	for _, component := range strings.Split(strings.TrimPrefix(root, "/"), "/") {
		if err := ctx.Err(); err != nil {
			return Identity{}, err
		}
		next, err := system.OpenComponent(current, component, how)
		if err != nil {
			return Identity{}, rootOperationError("resolve configured component", err)
		}
		if err := system.Close(current); err != nil {
			_ = system.Close(next)
			open = false
			return Identity{}, rootOperationError("close traversed descriptor", err)
		}
		current = next
	}
	var stat unix.Stat_t
	if err := system.Stat(current, &stat); err != nil {
		return Identity{}, rootOperationError("inspect configured root", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != 0 || stat.Gid != 0 || stat.Mode&0o7777 != 0o700 {
		return Identity{}, errs.New(
			errs.KindInternal,
			"environment volume root must be a root-owned directory with exact mode 0700",
		)
	}
	identity := Identity{Device: uint64(stat.Dev), Inode: stat.Ino}
	if identity.Device == 0 || identity.Inode == 0 {
		return Identity{}, errs.New(errs.KindInternal, "environment volume root identity is invalid")
	}
	if err := system.Close(current); err != nil {
		open = false
		return Identity{}, rootOperationError("close configured root descriptor", err)
	}
	open = false
	return identity, nil
}

func rootOperationError(operation string, err error) error {
	return errs.Wrap(errs.KindInternal, fmt.Errorf("environment volume root: %s: %w", operation, err))
}
