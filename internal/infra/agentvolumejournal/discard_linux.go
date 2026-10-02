//go:build linux

package agentvolumejournal

import (
	"context"
	"errors"

	"golang.org/x/sys/unix"
)

// Discard removes a zero-mutation journal only after the caller has a durable
// exact terminal FailedSafe disposition. Any completed intent or mutation must
// retain its journal for physical sibling/live-tree reconciliation instead.
func Discard(ctx context.Context, config Config) error {
	if ctx == nil {
		return invalid("Volume journal discard context is required")
	}
	expected, err := encodeAuthority(config)
	if err != nil {
		return err
	}
	rootFD, found, err := openAgentRoot(ctx, false)
	if err != nil || !found {
		return err
	}
	defer unix.Close(rootFD)
	if err := validateStageFilesystem(rootFD); err != nil {
		return err
	}
	fd, err := openExistingDirectory(rootFD, config.RestoreGenerationID)
	if errors.Is(err, unix.ENOENT) {
		return ResumeDiscard(ctx, config)
	}
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return conflict("Volume journal discard is already owned")
		}
		return storage(err)
	}
	names, err := directoryNames(fd)
	if err != nil {
		return err
	}
	var hasAuthority, hasLog bool
	for _, name := range names {
		switch name {
		case authorityName:
			hasAuthority = true
		case logName:
			hasLog = true
		default:
			return invalid("Volume journal discard has an unexpected leaf")
		}
	}
	if !hasAuthority {
		return invalid("Volume journal discard lacks sealed authority")
	}
	stored, err := readAuthority(ctx, fd)
	if err != nil {
		return err
	}
	if stored != config {
		return conflict("Volume journal discard authority differs")
	}
	if hasLog {
		file, err := openLeaf(fd, logName, unix.O_RDONLY, false, MaximumJournalBytes)
		if err != nil {
			return err
		}
		raw, readErr := readLogBytes(file)
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, storage(closeErr))
		}
		state, _, err := parseLog(raw, config, initialChain(expected), false)
		if err != nil {
			return err
		}
		if state.RecordCount != 0 || state.Pending != nil || state.RootDeleted || state.Exchanged {
			return conflict("Volume journal has mutation authority and cannot be discarded")
		}
	}
	if err := unix.Renameat2(rootFD, config.RestoreGenerationID, rootFD,
		config.RestoreGenerationID+".retiring", unix.RENAME_NOREPLACE); err != nil {
		return storage(err)
	}
	if err := unix.Fsync(rootFD); err != nil {
		return storage(err)
	}
	return cleanupRetiring(ctx, rootFD, fd, config, false)
}
