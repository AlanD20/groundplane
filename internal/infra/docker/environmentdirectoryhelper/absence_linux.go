//go:build linux

package environmentdirectoryhelper

import (
	"context"
	"errors"
	"syscall"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// Absent verifies absence beneath the configured, root-owned directory. A
// symlink, inaccessible ancestor or replaced directory is an error, not proof
// that deletion completed. No path outside the parsed Environment is inspected.
func (Creator) Absent(ctx context.Context, volumeRoot, volumeDirectory string) (bool, error) {
	if ctx == nil {
		return false, errs.New(errs.KindInternal, "Environment directory inspection requires context")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	scope, err := environmentpath.Parse(volumeRoot, volumeDirectory)
	if err != nil {
		return false, err
	}
	fd, err := unix.Open(volumeRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return false, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	defer func() { _ = unix.Close(fd) }() // Close the current pinned directory.
	if err := validateDirectory(fd, 0); err != nil {
		return false, err
	}
	components := []string{scope.TenantID, scope.ProjectID, scope.EnvironmentID}
	if scope.ProjectKind == environmentpath.ProjectKindBacking {
		components[0] = "platform"
	}
	for _, component := range components {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		next, err := openDirectoryAt(fd, component)
		if errors.Is(err, syscall.ENOENT) {
			return true, nil
		}
		if err != nil {
			return false, errs.Wrap(errs.KindStorageUnavailable, err)
		}
		if err := validateDirectory(next, 0); err != nil {
			_ = unix.Close(next) // Reject untrusted ownership before traversing.
			return false, err
		}
		if err := unix.Close(fd); err != nil {
			fd = next
			return false, errs.Wrap(errs.KindStorageUnavailable, err)
		}
		fd = next
	}
	return false, nil
}
