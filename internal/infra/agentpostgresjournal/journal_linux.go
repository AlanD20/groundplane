//go:build linux

package agentpostgresjournal

import (
	"context"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const resolveLocal = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS |
	unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV

// Mark durably records one exact Controller-owned database execution. The
// empty private directory is the complete marker; there is no partial payload.
func Mark(ctx context.Context, value IDs) (failure error) {
	if err := validateRequest(ctx, value); err != nil {
		return err
	}
	rootFD, found, err := openRoot(ctx, true)
	if err != nil {
		return err
	}
	if !found {
		return invalid("database execution journal root is unavailable")
	}
	defer closeDescriptor(rootFD, &failure)
	if err := lockRoot(ctx, rootFD); err != nil {
		return err
	}
	_, names, err := inventoryLocked(ctx, rootFD)
	if err != nil {
		return err
	}
	name := value.name()
	for _, existing := range names {
		if existing == name {
			return storage(unix.Fsync(rootFD))
		}
	}
	if len(names) >= maximumMarkers {
		return conflict("database execution journal marker capacity is exhausted")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := unix.Mkdirat(rootFD, name, 0o700); err != nil {
		return storage(err)
	}
	markerFD, _, err := openMarker(rootFD, name)
	if err != nil {
		return err
	}
	if err := markerEmpty(markerFD); err != nil {
		_ = unix.Close(markerFD)
		return err
	}
	if err := unix.Close(markerFD); err != nil {
		return storage(err)
	}
	return storage(unix.Fsync(rootFD))
}

// Inventory returns every exact execution marker in deterministic name order.
// Any unknown or unsafe entry rejects the complete inventory.
func Inventory(ctx context.Context) (result []IDs, failure error) {
	if ctx == nil {
		return nil, invalid("database execution journal context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rootFD, found, err := openRoot(ctx, false)
	if err != nil {
		return nil, err
	}
	if !found {
		return []IDs{}, nil
	}
	defer closeDescriptor(rootFD, &failure)
	if err := lockRoot(ctx, rootFD); err != nil {
		return nil, err
	}
	rows, _, err := inventoryLocked(ctx, rootFD)
	return rows, err
}

// Remove durably removes only the exact verified empty marker. Absence is an
// idempotent success; unexpected root entries still reject the operation.
func Remove(ctx context.Context, value IDs) (failure error) {
	if err := validateRequest(ctx, value); err != nil {
		return err
	}
	rootFD, found, err := openRoot(ctx, false)
	if err != nil || !found {
		return err
	}
	defer closeDescriptor(rootFD, &failure)
	if err := lockRoot(ctx, rootFD); err != nil {
		return err
	}
	_, names, err := inventoryLocked(ctx, rootFD)
	if err != nil {
		return err
	}
	name := value.name()
	present := false
	for _, existing := range names {
		if existing == name {
			present = true
			break
		}
	}
	if !present {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	markerFD, identity, err := openMarker(rootFD, name)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := unix.Close(markerFD); closeErr != nil && failure == nil {
			failure = storage(closeErr)
		}
	}()
	if err := markerEmpty(markerFD); err != nil {
		return err
	}
	var current unix.Stat_t
	if err := unix.Fstatat(rootFD, name, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return storage(err)
	}
	if current.Dev != identity.Dev || current.Ino != identity.Ino {
		return invalid("database execution journal marker changed")
	}
	if err := unix.Unlinkat(rootFD, name, unix.AT_REMOVEDIR); err != nil {
		return storage(err)
	}
	return storage(unix.Fsync(rootFD))
}

func validateRequest(ctx context.Context, value IDs) error {
	if ctx == nil {
		return invalid("database execution journal context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return value.validate()
}

func inventoryLocked(ctx context.Context, rootFD int) ([]IDs, []string, error) {
	names, err := directoryNames(rootFD, maximumMarkers+1)
	if err != nil {
		return nil, nil, err
	}
	if len(names) > maximumMarkers {
		return nil, nil, invalid("database execution journal inventory exceeds its bound")
	}
	sort.Strings(names)
	rows := make([]IDs, 0, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		value, err := parseName(name)
		if err != nil {
			return nil, nil, err
		}
		markerFD, _, err := openMarker(rootFD, name)
		if err != nil {
			return nil, nil, err
		}
		emptyErr := markerEmpty(markerFD)
		closeErr := unix.Close(markerFD)
		if emptyErr != nil {
			return nil, nil, emptyErr
		}
		if closeErr != nil {
			return nil, nil, storage(closeErr)
		}
		rows = append(rows, value)
	}
	return rows, names, nil
}

func directoryNames(fd, limit int) ([]string, error) {
	dup, err := unix.Openat2(fd, ".", &unix.OpenHow{
		Flags:   uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Resolve: resolveLocal,
	})
	if err != nil {
		return nil, storage(err)
	}
	directory := os.NewFile(uintptr(dup), "database-execution-journal")
	names, readErr := directory.Readdirnames(limit)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, storage(errors.Join(readErr, closeErr))
	}
	if closeErr != nil {
		return nil, storage(closeErr)
	}
	return names, nil
}

func openMarker(rootFD int, name string) (int, unix.Stat_t, error) {
	var named unix.Stat_t
	if err := unix.Fstatat(rootFD, name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return -1, unix.Stat_t{}, storage(err)
	}
	if !privateDirectory(named) {
		return -1, unix.Stat_t{}, invalid("database execution journal marker is unsafe")
	}
	fd, err := unix.Openat2(rootFD, name, &unix.OpenHow{
		Flags:   uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Resolve: resolveLocal,
	})
	if err != nil {
		return -1, unix.Stat_t{}, storage(err)
	}
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil {
		_ = unix.Close(fd)
		return -1, unix.Stat_t{}, storage(err)
	}
	if !privateDirectory(opened) || opened.Dev != named.Dev || opened.Ino != named.Ino {
		_ = unix.Close(fd)
		return -1, unix.Stat_t{}, invalid("database execution journal marker changed or is unsafe")
	}
	return fd, opened, nil
}

func markerEmpty(fd int) error {
	names, err := directoryNames(fd, 1)
	if err != nil {
		return err
	}
	if len(names) != 0 {
		return invalid("database execution journal marker is not empty")
	}
	return nil
}

func privateDirectory(value unix.Stat_t) bool {
	return value.Mode&unix.S_IFMT == unix.S_IFDIR && value.Mode&0o7777 == 0o700 &&
		value.Uid == 0 && value.Gid == 0
}

func openRoot(ctx context.Context, create bool) (int, bool, error) {
	current, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, false, storage(err)
	}
	var root unix.Stat_t
	if err := unix.Fstat(current, &root); err != nil || !safeAncestor(root) {
		_ = unix.Close(current)
		if err != nil {
			return -1, false, storage(err)
		}
		return -1, false, invalid("database execution journal root ancestry is unsafe")
	}
	parts := strings.Split(strings.TrimPrefix(rootPath, "/"), "/")
	for index, part := range parts {
		if err := ctx.Err(); err != nil {
			_ = unix.Close(current)
			return -1, false, err
		}
		final := index == len(parts)-1
		var before unix.Stat_t
		statErr := unix.Fstatat(current, part, &before, unix.AT_SYMLINK_NOFOLLOW)
		if errors.Is(statErr, unix.ENOENT) && !create {
			_ = unix.Close(current)
			return -1, false, nil
		}
		if errors.Is(statErr, unix.ENOENT) && final && create {
			if err := unix.Mkdirat(current, part, 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
				_ = unix.Close(current)
				return -1, false, storage(err)
			}
			if err := unix.Fsync(current); err != nil {
				_ = unix.Close(current)
				return -1, false, storage(err)
			}
			statErr = unix.Fstatat(current, part, &before, unix.AT_SYMLINK_NOFOLLOW)
		}
		if errors.Is(statErr, unix.ENOENT) {
			_ = unix.Close(current)
			return -1, false, invalid("database execution journal root ancestry is unavailable")
		}
		if statErr != nil {
			_ = unix.Close(current)
			return -1, false, storage(statErr)
		}
		if final && !privateDirectory(before) || !final && !safeAncestor(before) {
			_ = unix.Close(current)
			return -1, false, invalid("database execution journal root ancestry is unsafe")
		}
		resolve := uint64(unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS)
		if final {
			resolve = resolveLocal
		}
		next, err := unix.Openat2(current, part, &unix.OpenHow{
			Flags:   uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW),
			Resolve: resolve,
		})
		_ = unix.Close(current)
		if err != nil {
			return -1, false, storage(err)
		}
		var after unix.Stat_t
		if err := unix.Fstat(next, &after); err != nil {
			_ = unix.Close(next)
			return -1, false, storage(err)
		}
		if after.Dev != before.Dev || after.Ino != before.Ino ||
			final && !privateDirectory(after) || !final && !safeAncestor(after) {
			_ = unix.Close(next)
			return -1, false, invalid("database execution journal root ancestor changed or is unsafe")
		}
		current = next
	}
	return current, true, nil
}

func safeAncestor(value unix.Stat_t) bool {
	return value.Mode&unix.S_IFMT == unix.S_IFDIR && value.Mode&0o022 == 0 && value.Uid == 0
}

func lockRoot(ctx context.Context, fd int) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			return storage(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func closeDescriptor(fd int, failure *error) {
	if err := unix.Close(fd); err != nil && *failure == nil {
		*failure = storage(err)
	}
}
