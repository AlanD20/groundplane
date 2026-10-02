//go:build linux

package agentvolumemanifest

import (
	"bytes"
	"context"
	"errors"

	"golang.org/x/sys/unix"
)

// Discard removes an incomplete RESTORE_NEW metadata receiver only after the
// caller has a durable exact terminal FailedSafe Controller disposition. It
// never creates a missing active journal or treats complete input as discardable.
func Discard(ctx context.Context, config Config) error {
	if ctx == nil {
		return invalid("Volume manifest receiver discard context is required")
	}
	expected, err := encodeAuthority(config)
	if err != nil {
		return err
	}
	rootFD, found, err := openRoot(ctx, false)
	if err != nil || !found {
		return err
	}
	defer unix.Close(rootFD)
	if err := validateStageFilesystem(rootFD); err != nil {
		return err
	}
	fd, err := openDirectory(rootFD, config.Binding.TransferID, false)
	if errors.Is(err, unix.ENOENT) {
		return ResumeDiscard(ctx, config)
	}
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return conflict("Volume manifest receiver discard is already owned")
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
			return invalid("Volume manifest receiver discard has an unexpected leaf")
		}
	}
	if !hasAuthority {
		return invalid("Volume manifest receiver discard lacks sealed authority")
	}
	stored, err := readLeaf(fd, authorityName, maximumAuthority)
	if err != nil {
		return err
	}
	if !bytes.Equal(stored, expected) {
		return conflict("Volume manifest receiver discard authority differs")
	}
	if hasLog {
		raw, err := readLeaf(fd, logName, MaximumJournalBytes)
		if err != nil {
			return err
		}
		state, _, err := parseLog(raw, config, expected)
		if err != nil {
			return err
		}
		if state.complete {
			return conflict("Complete Volume manifest receiver cannot be discarded")
		}
	}
	if err := unix.Renameat2(rootFD, config.Binding.TransferID, rootFD,
		config.Binding.TransferID+".retiring", unix.RENAME_NOREPLACE); err != nil {
		return storage(err)
	}
	if err := unix.Fsync(rootFD); err != nil {
		return storage(err)
	}
	return cleanupRetiring(ctx, rootFD, fd, config, false)
}
