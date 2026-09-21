package backupstage

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"sort"
)

func directoryNames(ctx context.Context, fd int, label string) ([]string, error) {
	duplicate, err := unix.Dup(fd)
	if err != nil {
		return nil, systemError("duplicate "+label+" descriptor", err)
	}
	directory := os.NewFile(uintptr(duplicate), "backup-stage-directory")
	if directory == nil {
		return nil, closeWithPrimary(ctx, internalError("wrap "+label+" descriptor"), duplicate)
	}
	if _, err := unix.Seek(duplicate, 0, 0); err != nil {
		return nil, closeWithPrimary(ctx, systemError("rewind "+label, err), duplicate)
	}
	names, readErr := directory.Readdirnames(-1)
	closeErr := directory.Close()
	if readErr != nil {
		return nil, joinPrivate(systemError("read "+label, readErr), rawOperationError("close "+label, closeErr))
	}
	if closeErr != nil {
		return nil, systemError("close "+label, closeErr)
	}
	sort.Strings(names)
	return names, nil
}

func rejectExistingEntry(ctx context.Context, parentFD int, name string) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(parentFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		return internalError("managed staging namespace is ambiguous")
	} else if !errors.Is(err, unix.ENOENT) {
		return systemError("inspect existing staging entry", err)
	}
	return nil
}

func removePointContents(ctx context.Context, fd int, ops linuxOperations) error {
	names, err := directoryNames(ctx, fd, "point directory for cleanup")
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := contextError(ctx); err != nil {
			return err
		}
		entryFD, err := openPathAt(ctx, fd, name, ops)
		if err != nil {
			return systemError("inspect point entry for cleanup", err)
		}
		metadataErr := validateRegularFD(ctx, entryFD, -1, 1)
		closeErr := unix.Close(entryFD)
		if metadataErr != nil {
			return joinPrivate(internalError("managed point namespace is ambiguous during cleanup"), metadataErr,
				rawOperationError("close point entry cleanup descriptor", closeErr))
		}
		if closeErr != nil {
			return systemError("close point entry cleanup descriptor", closeErr)
		}
		if err := unix.Unlinkat(fd, name, 0); err != nil {
			return systemError("remove point entry", err)
		}
	}
	if err := unix.Fsync(fd); err != nil {
		return systemError("sync point directory after cleanup", err)
	}
	return nil
}
