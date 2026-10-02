//go:build linux

package agentvolumemanifest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	"golang.org/x/sys/unix"
)

// Cleanup retires only a complete, exact RESTORE_NEW manifest. The caller must
// first durably record the Controller disposition authorizing retirement.
func (journal *Journal) Cleanup(ctx context.Context) error {
	if journal == nil {
		return invalid("Volume manifest receiver is unavailable")
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.available(ctx); err != nil {
		return err
	}
	if !journal.state.complete || journal.state.partial {
		return conflict("Volume manifest receiver is not terminal")
	}
	active := journal.config.Binding.TransferID
	if err := unix.Renameat2(journal.rootFD, active, journal.rootFD,
		active+".retiring", unix.RENAME_NOREPLACE); err != nil {
		return storage(err)
	}
	journal.poisoned = true
	if err := unix.Fsync(journal.rootFD); err != nil {
		return storage(err)
	}
	return cleanupRetiring(ctx, journal.rootFD, journal.directoryFD, journal.config, true)
}

// ResumeCleanup acts only on a .retiring namespace with the same sealed
// authority. It never creates or replaces a manifest on reconnect.
func ResumeCleanup(ctx context.Context, config Config) error {
	return resumeRetiring(ctx, config, true)
}

// ResumeDiscard finishes a Controller-authorized incomplete receiver discard.
// A normal completion tombstone is never accepted through this path.
func ResumeDiscard(ctx context.Context, config Config) error {
	return resumeRetiring(ctx, config, false)
}

func resumeRetiring(ctx context.Context, config Config, requireComplete bool) error {
	if ctx == nil {
		return invalid("Volume manifest receiver cleanup context is required")
	}
	if err := config.validate(); err != nil {
		return err
	}
	rootFD, found, err := openRoot(ctx, false)
	if err != nil || !found {
		return err
	}
	defer unix.Close(rootFD)
	fd, err := openDirectory(rootFD, config.Binding.TransferID+".retiring", false)
	if errors.Is(err, unix.ENOENT) {
		var active unix.Stat_t
		activeErr := unix.Fstatat(rootFD, config.Binding.TransferID, &active, unix.AT_SYMLINK_NOFOLLOW)
		if activeErr == nil {
			return conflict("Volume manifest receiver remains active, not retiring")
		}
		if errors.Is(activeErr, unix.ENOENT) {
			return nil
		}
		return storage(activeErr)
	}
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return conflict("Volume manifest receiver cleanup is already owned")
		}
		return storage(err)
	}
	return cleanupRetiring(ctx, rootFD, fd, config, requireComplete)
}

func cleanupRetiring(ctx context.Context, rootFD, fd int, config Config, requireComplete bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	names, err := directoryNames(fd)
	if err != nil {
		return err
	}
	markerFile := markerName
	if !requireComplete {
		markerFile = discardName
	}
	var hasAuthority, hasLog, hasMarker bool
	for _, name := range names {
		switch name {
		case authorityName:
			hasAuthority = true
		case logName:
			hasLog = true
		case markerFile:
			hasMarker = true
		default:
			return invalid("Retiring Volume manifest receiver contains an unexpected leaf")
		}
	}
	if hasLog && !hasAuthority || !hasAuthority && !hasMarker && len(names) != 0 {
		return invalid("Retiring Volume manifest receiver authority is ambiguous")
	}
	expected, err := encodeAuthority(config)
	if err != nil {
		return err
	}
	seed := initialHash(expected)
	marker := retirementMarker(expected, seed)
	if hasAuthority {
		stored, err := readLeaf(fd, authorityName, maximumAuthority)
		if err != nil {
			return err
		}
		if !bytes.Equal(stored, expected) {
			return conflict("Retiring Volume manifest receiver authority differs")
		}
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
		if state.complete != requireComplete || requireComplete && state.partial {
			return conflict("Retiring Volume manifest receiver disposition conflicts with its state")
		}
		marker = retirementMarker(expected, state.logHash)
	}
	if hasMarker {
		current, err := readLeaf(fd, markerFile, int64(len(marker)))
		if err != nil {
			return err
		}
		if len(current) > len(marker) ||
			!bytes.Equal(current[:min(len(current), 3+len(expected))], marker[:min(len(current), 3+len(expected))]) ||
			hasLog && !bytes.Equal(current, marker[:len(current)]) ||
			!hasLog && len(current) != len(marker) {
			return conflict("Retiring Volume manifest receiver marker differs")
		}
		if !hasLog {
			if _, err := markerAuthority(current); err != nil {
				return err
			}
			marker = current
		}
	} else if !hasLog && len(names) != 0 && (requireComplete || !hasAuthority) {
		return invalid("Retiring Volume manifest receiver has no terminal proof")
	}
	if hasLog || !requireComplete && hasAuthority && !hasMarker {
		file, err := openLeaf(fd, markerFile, unix.O_RDWR, !hasMarker, int64(len(marker)))
		if err != nil {
			return err
		}
		current := int64(0)
		if hasMarker {
			info, err := file.Stat()
			if err != nil {
				_ = file.Close()
				return storage(err)
			}
			current = info.Size()
		}
		written, writeErr := file.WriteAt(marker[current:], current)
		if writeErr == nil && written != len(marker)-int(current) {
			writeErr = io.ErrShortWrite
		}
		if writeErr == nil {
			writeErr = file.Sync()
		}
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			return storage(errors.Join(writeErr, closeErr))
		}
		if err := unix.Fsync(fd); err != nil {
			return storage(err)
		}
		hasMarker = true
	}
	if hasLog {
		if err := unix.Unlinkat(fd, logName, 0); err != nil {
			return storage(err)
		}
		if err := unix.Fsync(fd); err != nil {
			return storage(err)
		}
	}
	if hasAuthority {
		if err := unix.Unlinkat(fd, authorityName, 0); err != nil {
			return storage(err)
		}
		if err := unix.Fsync(fd); err != nil {
			return storage(err)
		}
	}
	if hasMarker {
		if err := unix.Unlinkat(fd, markerFile, 0); err != nil {
			return storage(err)
		}
		if err := unix.Fsync(fd); err != nil {
			return storage(err)
		}
	}
	if err := unix.Unlinkat(rootFD, config.Binding.TransferID+".retiring", unix.AT_REMOVEDIR); err != nil {
		return storage(err)
	}
	return storage(unix.Fsync(rootFD))
}

// The marker retains canonical Config after authority.bin is unlinked.
func retirementMarker(authority []byte, chain [sha256.Size]byte) []byte {
	marker := make([]byte, 3+len(authority)+2*sha256.Size)
	marker[0] = 1
	binary.BigEndian.PutUint16(marker[1:3], uint16(len(authority)))
	copy(marker[3:], authority)
	copy(marker[3+len(authority):], chain[:])
	digest := sha256.Sum256(append([]byte("groundplane.agent.volume-manifest.retirement.v1\x00"),
		marker[:3+len(authority)+sha256.Size]...))
	copy(marker[3+len(authority)+sha256.Size:], digest[:])
	return marker
}

func markerAuthority(raw []byte) (Config, error) {
	if len(raw) < 3+2*sha256.Size || raw[0] != 1 {
		return Config{}, invalid("Volume manifest receiver retirement marker is invalid")
	}
	length := int(binary.BigEndian.Uint16(raw[1:3]))
	if length == 0 || length > maximumAuthority || len(raw) != 3+length+2*sha256.Size {
		return Config{}, invalid("Volume manifest receiver retirement marker length is invalid")
	}
	digest := sha256.Sum256(append([]byte("groundplane.agent.volume-manifest.retirement.v1\x00"),
		raw[:3+length+sha256.Size]...))
	if !bytes.Equal(digest[:], raw[3+length+sha256.Size:]) {
		return Config{}, invalid("Volume manifest receiver retirement marker checksum is invalid")
	}
	return decodeAuthority(raw[3 : 3+length])
}
