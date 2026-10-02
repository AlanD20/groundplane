//go:build linux

// Package backupvolumefs owns descriptor-confined access to one authorized
// managed Volume tree. Callers own the sealed assignment and transfer journal.
package backupvolumefs

import (
	"context"
	"fmt"
	"path"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const confined = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV

// Volume holds the Environment parent and live directory descriptors. The
// caller must keep the Volume open for the entire capture or restore procedure.
type Volume struct {
	parentFD int
	liveFD   int
	name     string
	dev      uint64
	ino      uint64
}

// Open accepts only the derived Environment directory under the configured
// root and the sealed Compose volume key. Root ancestors may cross filesystems;
// every descendant of the configured root is confined to its filesystem.
func Open(ctx context.Context, volumeRoot, authorizedVolumeDir, composeKey string) (*Volume, error) {
	if ctx == nil {
		return nil, invalid("managed Volume context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := environmentpath.Parse(volumeRoot, authorizedVolumeDir); err != nil {
		return nil, err
	}
	if !validName(composeKey) {
		return nil, invalid("managed Volume compose key is invalid")
	}
	rootFD, err := openAbsoluteRoot(ctx, volumeRoot)
	if err != nil {
		return nil, err
	}
	defer unix.Close(rootFD)
	relative := strings.TrimPrefix(authorizedVolumeDir, volumeRoot+"/")
	parentFD, err := openDirectory(rootFD, relative)
	if err != nil {
		return nil, system("open authorized Environment Volume directory", err)
	}
	liveFD, err := openDirectory(parentFD, composeKey)
	if err != nil {
		_ = unix.Close(parentFD)
		return nil, system("open managed Volume", err)
	}
	var parent, live unix.Stat_t
	if err := unix.Fstat(parentFD, &parent); err != nil {
		_ = unix.Close(liveFD)
		_ = unix.Close(parentFD)
		return nil, system("inspect managed Volume parent", err)
	}
	if err := unix.Fstat(liveFD, &live); err != nil {
		_ = unix.Close(liveFD)
		_ = unix.Close(parentFD)
		return nil, system("inspect managed Volume", err)
	}
	if parent.Dev != live.Dev || parent.Mode&unix.S_IFMT != unix.S_IFDIR || live.Mode&unix.S_IFMT != unix.S_IFDIR ||
		parent.Uid != 0 || parent.Mode&0o7777 != 0o700 {
		_ = unix.Close(liveFD)
		_ = unix.Close(parentFD)
		return nil, invalid("managed Volume parent or mount identity is invalid")
	}
	return &Volume{parentFD: parentFD, liveFD: liveFD, name: composeKey, dev: uint64(live.Dev), ino: live.Ino}, nil
}

func (volume *Volume) Close() error {
	if volume == nil || volume.name == "" {
		return nil
	}
	if volume.liveFD < 0 && volume.parentFD < 0 {
		return nil
	}
	first := unix.Close(volume.liveFD)
	second := unix.Close(volume.parentFD)
	volume.liveFD, volume.parentFD = -1, -1
	if first != nil {
		return system("close managed Volume", first)
	}
	if second != nil {
		return system("close managed Volume parent", second)
	}
	return nil
}

func openAbsoluteRoot(ctx context.Context, root string) (int, error) {
	if err := environmentpath.ValidateRoot(root); err != nil {
		return -1, err
	}
	current, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, system("open filesystem root", err)
	}
	for _, component := range strings.Split(strings.TrimPrefix(root, "/"), "/") {
		if err := ctx.Err(); err != nil {
			_ = unix.Close(current)
			return -1, err
		}
		next, openErr := unix.Openat2(current, component, &unix.OpenHow{
			Flags:   uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW),
			Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS,
		})
		_ = unix.Close(current)
		if openErr != nil {
			return -1, system("resolve managed Volume root", openErr)
		}
		current = next
	}
	var stat unix.Stat_t
	if err := unix.Fstat(current, &stat); err != nil {
		_ = unix.Close(current)
		return -1, system("inspect managed Volume root", err)
	}
	if stat.Uid != 0 || stat.Mode&0o7777 != 0o700 || stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		_ = unix.Close(current)
		return -1, invalid("managed Volume root must be root-owned mode 0700")
	}
	return current, nil
}

func openDirectory(parentFD int, relative string) (int, error) {
	return unix.Openat2(parentFD, relative, &unix.OpenHow{
		Flags: uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: confined,
	})
}

func openRegular(parentFD int, relative string) (int, error) {
	return unix.Openat2(parentFD, relative, &unix.OpenHow{
		Flags: uint64(unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK), Resolve: confined,
	})
}

func validName(name string) bool {
	if name == "" || name == "." || name == ".." || len(name) > 255 {
		return false
	}
	for _, c := range name {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

func parentAndName(relative string) (string, string) {
	return path.Dir(relative), path.Base(relative)
}

func invalid(message string) error { return errs.New(errs.KindValidationFailed, message) }

func system(message string, cause error) error {
	return errs.Wrap(errs.KindInternal, fmt.Errorf("%s: %w", message, cause))
}
