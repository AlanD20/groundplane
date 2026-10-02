//go:build linux

package agentvolumejournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	"golang.org/x/sys/unix"
)

// Cleanup retires a journal only after the durable root-deletion completion.
// Rename and each removal are synced; a crash in the middle leaves a visible
// .retiring directory for exact ResumeCleanup, never an apparently new journal.
func (journal *Journal) Cleanup(ctx context.Context) error {
	if journal == nil {
		return invalid("Volume journal is unavailable")
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.available(ctx); err != nil {
		return err
	}
	if !journal.state.RootDeleted || journal.state.Pending != nil || journal.state.UncommittedPrefix {
		return conflict("Volume journal mutation history is not terminal")
	}
	active := journal.config.RestoreGenerationID
	retiring := active + ".retiring"
	if err := unix.Renameat2(journal.rootFD, active, journal.rootFD, retiring, unix.RENAME_NOREPLACE); err != nil {
		return storage(err)
	}
	journal.poisoned = true
	if err := unix.Fsync(journal.rootFD); err != nil {
		return storage(err)
	}
	return cleanupRetiring(ctx, journal.rootFD, journal.directoryFD, journal.config, true)
}

// ResumeCleanup finishes an already-retiring generation whose authority still
// matches the exact sealed assignment. It never deletes an active journal.
func ResumeCleanup(ctx context.Context, config Config) error {
	return resumeRetiring(ctx, config, true)
}

// ResumeDiscard finishes only an exact zero-mutation terminal discard.
func ResumeDiscard(ctx context.Context, config Config) error {
	return resumeRetiring(ctx, config, false)
}

func resumeRetiring(ctx context.Context, config Config, requireComplete bool) error {
	if ctx == nil {
		return invalid("Volume journal cleanup context is required")
	}
	if err := config.validate(); err != nil {
		return err
	}
	rootFD, found, err := openAgentRoot(ctx, false)
	if err != nil || !found {
		return err
	}
	defer unix.Close(rootFD)
	fd, err := openExistingDirectory(rootFD, config.RestoreGenerationID+".retiring")
	if errors.Is(err, unix.ENOENT) {
		var active unix.Stat_t
		activeErr := unix.Fstatat(rootFD, config.RestoreGenerationID, &active, unix.AT_SYMLINK_NOFOLLOW)
		if activeErr == nil {
			return conflict("Volume journal remains active, not retiring")
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
			return conflict("Volume journal cleanup is already owned")
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
	markerFile := retireName
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
			return invalid("Retiring Volume journal contains an unexpected leaf")
		}
	}
	if hasLog && !hasAuthority || !hasAuthority && !hasMarker && len(names) != 0 {
		return invalid("Retiring Volume journal has an ambiguous authority")
	}
	encoded, err := encodeAuthority(config)
	if err != nil {
		return err
	}
	seed := initialChain(encoded)
	marker := retirementMarker(encoded, seed)
	if hasAuthority {
		stored, err := readAuthority(ctx, fd)
		if err != nil {
			return err
		}
		if stored != config {
			return conflict("Retiring Volume journal authority differs from assignment")
		}
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
		state, _, err := parseLog(raw, config, seed, false)
		if err != nil {
			return err
		}
		if requireComplete && (!state.RootDeleted || state.Pending != nil || state.UncommittedPrefix) ||
			!requireComplete && state.RecordCount != 0 {
			return conflict("Retiring Volume journal disposition conflicts with mutations")
		}
		marker = retirementMarker(encoded, state.ChainSHA256)
	}
	if hasMarker {
		file, err := openLeaf(fd, markerFile, unix.O_RDONLY, false, int64(len(marker)))
		if err != nil {
			return err
		}
		current, readErr := io.ReadAll(io.LimitReader(file, int64(len(marker))+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, storage(closeErr))
		}
		if len(current) > len(marker) ||
			!bytes.Equal(current[:min(len(current), 3+len(encoded))], marker[:min(len(current), 3+len(encoded))]) {
			return conflict("Retiring Volume journal marker differs from assignment")
		}
		if hasLog && !bytes.Equal(current, marker[:len(current)]) {
			return conflict("Retiring Volume journal marker differs from terminal chain")
		}
		if !hasLog && len(current) != len(marker) {
			return invalid("Retiring Volume journal marker is incomplete without its log")
		}
		if !hasLog {
			if _, err := markerAuthority(current); err != nil {
				return err
			}
			marker = current
		}
	} else if !hasLog && len(names) != 0 && (requireComplete || !hasAuthority) {
		return invalid("Retiring Volume journal has no terminal proof")
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
	if err := unix.Unlinkat(rootFD, config.RestoreGenerationID+".retiring", unix.AT_REMOVEDIR); err != nil {
		return storage(err)
	}
	return storage(unix.Fsync(rootFD))
}

func retirementMarker(authority []byte, chain [sha256.Size]byte) []byte {
	marker := make([]byte, 3+len(authority)+2*sha256.Size)
	marker[0] = 1
	binary.BigEndian.PutUint16(marker[1:3], uint16(len(authority)))
	copy(marker[3:], authority)
	copy(marker[3+len(authority):], chain[:])
	digest := sha256.Sum256(append([]byte("groundplane.agent.volume-journal.retirement.v1\x00"),
		marker[:3+len(authority)+sha256.Size]...))
	copy(marker[3+len(authority)+sha256.Size:], digest[:])
	return marker
}

func markerAuthority(raw []byte) (Config, error) {
	if len(raw) < 3+2*sha256.Size || raw[0] != 1 {
		return Config{}, invalid("Volume journal retirement marker is invalid")
	}
	length := int(binary.BigEndian.Uint16(raw[1:3]))
	if length == 0 || length > maximumAuthorityBytes || len(raw) != 3+length+2*sha256.Size {
		return Config{}, invalid("Volume journal retirement marker length is invalid")
	}
	digest := sha256.Sum256(append([]byte("groundplane.agent.volume-journal.retirement.v1\x00"),
		raw[:3+length+sha256.Size]...))
	if !bytes.Equal(digest[:], raw[3+length+sha256.Size:]) {
		return Config{}, invalid("Volume journal retirement marker checksum is invalid")
	}
	return decodeAuthority(raw[3 : 3+length])
}

func readRetirementMarker(fd int, name string) ([]byte, error) {
	file, err := openLeaf(fd, name, unix.O_RDONLY, false, maximumAuthorityBytes+3+2*sha256.Size)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maximumAuthorityBytes+3+2*sha256.Size+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, storage(errors.Join(readErr, closeErr))
	}
	return raw, nil
}
