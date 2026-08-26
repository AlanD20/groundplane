//go:build linux

package environmentdirectoryhelper

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Creator struct{}

func (Creator) Create(ctx context.Context, volumeRoot string, volumeDirectory string) error {
	return createDirectories(ctx, volumeRoot, volumeDirectory, 0)
}

func (Creator) Remove(ctx context.Context, volumeRoot string, volumeDirectory string) error {
	return removeDirectory(ctx, volumeRoot, volumeDirectory, 0)
}

func createDirectories(
	ctx context.Context,
	volumeRoot string,
	volumeDirectory string,
	expectedUID uint32,
) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "Environment directory context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	scope, err := environmentpath.Parse(volumeRoot, volumeDirectory)
	if err != nil {
		return err
	}
	rootFD, err := unix.Open(volumeRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("open Environment volume root: %w", err))
	}
	if err := validateDirectory(rootFD, expectedUID); err != nil {
		_ = unix.Close(rootFD)
		return err
	}
	components := []string{scope.TenantID, scope.ProjectID, scope.EnvironmentID}
	if scope.ProjectKind == environmentpath.ProjectKindBacking {
		components[0] = "platform"
	}
	parentFD := rootFD
	for _, component := range components {
		if err := ctx.Err(); err != nil {
			_ = unix.Close(parentFD)
			return err
		}
		nextFD, openErr := openOrCreateDirectory(parentFD, component, expectedUID)
		closeErr := unix.Close(parentFD)
		if openErr != nil {
			return openErr
		}
		if closeErr != nil {
			_ = unix.Close(nextFD)
			return errs.Wrap(errs.KindInternal, closeErr)
		}
		parentFD = nextFD
	}
	if err := unix.Close(parentFD); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func openOrCreateDirectory(parentFD int, name string, expectedUID uint32) (int, error) {
	fd, err := openDirectoryAt(parentFD, name)
	created := false
	if errors.Is(err, syscall.ENOENT) {
		mkdirErr := unix.Mkdirat(parentFD, name, 0o700)
		if mkdirErr != nil && !errors.Is(mkdirErr, syscall.EEXIST) {
			return -1, errs.Wrap(errs.KindInternal, fmt.Errorf("create Environment directory: %w", mkdirErr))
		}
		created = mkdirErr == nil
		fd, err = openDirectoryAt(parentFD, name)
	}
	if err != nil {
		return -1, errs.Wrap(errs.KindInternal, fmt.Errorf("open Environment directory: %w", err))
	}
	if created {
		if err := unix.Fchmod(fd, 0o700); err != nil {
			_ = unix.Close(fd)
			return -1, errs.Wrap(errs.KindInternal, fmt.Errorf("secure Environment directory: %w", err))
		}
	}
	if err := validateDirectory(fd, expectedUID); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

func openDirectoryAt(parentFD int, name string) (int, error) {
	return unix.Openat2(parentFD, name, &unix.OpenHow{
		Flags: uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS |
			unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV,
	})
}

func validateDirectory(fd int, expectedUID uint32) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Mode&0o777 != 0o700 || stat.Uid != expectedUID {
		return errs.New(errs.KindValidationFailed, "Environment directory must be owner-owned mode 0700")
	}
	return nil
}

func removeDirectory(
	ctx context.Context,
	volumeRoot string,
	volumeDirectory string,
	expectedUID uint32,
) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "Environment directory removal context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	scope, err := environmentpath.Parse(volumeRoot, volumeDirectory)
	if err != nil {
		return err
	}
	rootFD, err := unix.Open(volumeRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("open Environment volume root: %w", err))
	}
	if err := validateDirectory(rootFD, expectedUID); err != nil {
		_ = unix.Close(rootFD)
		return err
	}
	components := []string{scope.TenantID, scope.ProjectID}
	if scope.ProjectKind == environmentpath.ProjectKindBacking {
		components[0] = "platform"
	}
	parentFD := rootFD
	for _, component := range components {
		if err := ctx.Err(); err != nil {
			_ = unix.Close(parentFD)
			return err
		}
		nextFD, openErr := openDirectoryAt(parentFD, component)
		closeErr := unix.Close(parentFD)
		if errors.Is(openErr, syscall.ENOENT) {
			return nil
		}
		if openErr != nil {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("open Environment directory ancestor: %w", openErr))
		}
		if closeErr != nil {
			_ = unix.Close(nextFD)
			return errs.Wrap(errs.KindInternal, closeErr)
		}
		if err := validateDirectory(nextFD, expectedUID); err != nil {
			_ = unix.Close(nextFD)
			return err
		}
		parentFD = nextFD
	}
	environmentFD, err := openDirectoryAt(parentFD, scope.EnvironmentID)
	if errors.Is(err, syscall.ENOENT) {
		_ = unix.Close(parentFD)
		return nil
	}
	if err != nil {
		_ = unix.Close(parentFD)
		return errs.Wrap(errs.KindInternal, fmt.Errorf("open Environment directory for removal: %w", err))
	}
	if err := validateDirectory(environmentFD, expectedUID); err != nil {
		_ = unix.Close(environmentFD)
		_ = unix.Close(parentFD)
		return err
	}
	if err := removeOpenDirectoryContents(ctx, environmentFD); err != nil {
		_ = unix.Close(parentFD)
		return err
	}
	if err := unix.Unlinkat(parentFD, scope.EnvironmentID, unix.AT_REMOVEDIR); err != nil {
		_ = unix.Close(parentFD)
		return errs.Wrap(errs.KindInternal, fmt.Errorf("remove Environment directory: %w", err))
	}
	if err := unix.Fsync(parentFD); err != nil {
		_ = unix.Close(parentFD)
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := unix.Close(parentFD); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

// removeOpenDirectoryContents takes ownership of fd. It never follows a
// symlink: directories are reopened beneath their parent and every other file
// type is unlinked as a leaf.
func removeOpenDirectoryContents(ctx context.Context, fd int) error {
	directory := os.NewFile(uintptr(fd), "environment-directory")
	if directory == nil {
		_ = unix.Close(fd)
		return errs.New(errs.KindInternal, "open Environment directory stream failed")
	}
	defer directory.Close()
	names, err := directory.Readdirnames(-1)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		var stat unix.Stat_t
		if err := unix.Fstatat(fd, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("inspect Environment directory entry: %w", err))
		}
		if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
			childFD, err := openDirectoryAt(fd, name)
			if err != nil {
				return errs.Wrap(errs.KindInternal, fmt.Errorf("open Environment directory child: %w", err))
			}
			if err := removeOpenDirectoryContents(ctx, childFD); err != nil {
				return err
			}
			if err := unix.Unlinkat(fd, name, unix.AT_REMOVEDIR); err != nil {
				return errs.Wrap(errs.KindInternal, fmt.Errorf("remove Environment directory child: %w", err))
			}
			continue
		}
		if err := unix.Unlinkat(fd, name, 0); err != nil {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("remove Environment directory entry: %w", err))
		}
	}
	if err := unix.Fsync(fd); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
