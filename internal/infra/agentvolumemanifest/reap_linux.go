//go:build linux

package agentvolumemanifest

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/oklog/ulid/v2"
	"golang.org/x/sys/unix"
)

// ReapEmptyRetired removes only empty, inode-checked .retiring directories.
// A completely emptied tombstone is the sole crash gap with no authority leaf;
// the rename occurred only after a Controller-authorized retirement/discard.
func ReapEmptyRetired(ctx context.Context, root string) error {
	if ctx == nil || root != AgentRoot {
		return invalid("Volume manifest receiver reap root is invalid")
	}
	rootFD, found, err := openRoot(ctx, false)
	if err != nil || !found {
		return err
	}
	defer unix.Close(rootFD)
	if err := validateStageFilesystem(rootFD); err != nil {
		return err
	}
	dup, err := unix.Dup(rootFD)
	if err != nil {
		return storage(err)
	}
	directory := os.NewFile(uintptr(dup), "volume-manifest-reap-root")
	names, readErr := directory.Readdirnames(MaximumJournals + 1)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != nil {
		return storage(errors.Join(readErr, closeErr))
	}
	if len(names) > MaximumJournals {
		return invalid("Volume manifest receiver reap inventory exceeds its bound")
	}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !strings.HasSuffix(name, ".retiring") {
			continue
		}
		transfer := strings.TrimSuffix(name, ".retiring")
		if len(transfer) != 26 || transfer != strings.ToUpper(transfer) {
			return invalid("Volume manifest receiver retiring name is invalid")
		}
		if _, err := ulid.ParseStrict(transfer); err != nil {
			return invalid("Volume manifest receiver retiring ID is invalid")
		}
		fd, err := openDirectory(rootFD, name, false)
		if err != nil {
			return err
		}
		if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
			_ = unix.Close(fd)
			if errors.Is(err, unix.EWOULDBLOCK) {
				return conflict("Volume manifest receiver retiring directory is owned")
			}
			return storage(err)
		}
		leaves, readErr := directoryNames(fd)
		if readErr != nil {
			_ = unix.Close(fd)
			return readErr
		}
		if len(leaves) == 0 {
			if err := unix.Unlinkat(rootFD, name, unix.AT_REMOVEDIR); err != nil {
				_ = unix.Close(fd)
				return storage(err)
			}
			if err := unix.Fsync(rootFD); err != nil {
				_ = unix.Close(fd)
				return storage(err)
			}
		}
		if err := unix.Close(fd); err != nil {
			return storage(err)
		}
	}
	return nil
}
