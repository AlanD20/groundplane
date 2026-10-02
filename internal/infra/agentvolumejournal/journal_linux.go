//go:build linux

package agentvolumejournal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
)

const (
	authorityName = "authority.bin"
	logName       = "mutations.bin"
	retireName    = "retire.bin"
	discardName   = "discard.bin"
	resolveLocal  = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV
)

type Journal struct {
	mu             sync.Mutex
	rootFD         int
	directoryFD    int
	log            *os.File
	config         Config
	state          State
	tail           []byte
	committedBytes int64
	closed         bool
	poisoned       bool
}

var _ backupvolumefs.Journal = (*Journal)(nil)

// Open pins one generation directory and holds its exclusive process lock
// until Close. Existing authority must match every Config field exactly.
func Open(ctx context.Context, config Config) (*Journal, error) {
	if ctx == nil {
		return nil, invalid("Volume journal context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	rootFD, found, err := openAgentRoot(ctx, true)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, invalid("Volume journal root is unavailable")
	}
	if err := validateStageFilesystem(rootFD); err != nil {
		_ = unix.Close(rootFD)
		return nil, err
	}
	var retiring unix.Stat_t
	if err := unix.Fstatat(rootFD, config.RestoreGenerationID+".retiring", &retiring,
		unix.AT_SYMLINK_NOFOLLOW); err == nil {
		_ = unix.Close(rootFD)
		return nil, conflict("Volume journal generation is already retiring")
	} else if !errors.Is(err, unix.ENOENT) {
		_ = unix.Close(rootFD)
		return nil, storage(err)
	}
	directoryFD, err := openOrCreateDirectory(rootFD, config.RestoreGenerationID)
	if err != nil {
		_ = unix.Close(rootFD)
		return nil, err
	}
	cleanup := func() { _ = unix.Close(directoryFD); _ = unix.Close(rootFD) }
	if err := unix.Flock(directoryFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		cleanup()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, conflict("Volume journal is already open")
		}
		return nil, storage(err)
	}
	names, err := directoryNames(directoryFD)
	if err != nil {
		cleanup()
		return nil, err
	}
	var hasAuthority, hasLog bool
	for _, name := range names {
		switch name {
		case authorityName:
			hasAuthority = true
		case logName:
			hasLog = true
		default:
			cleanup()
			return nil, invalid("Volume journal contains an unexpected leaf")
		}
	}
	if hasLog && !hasAuthority {
		cleanup()
		return nil, invalid("Volume journal log lacks its sealed authority")
	}
	authority, err := encodeAuthority(config)
	if err != nil {
		cleanup()
		return nil, err
	}
	if err := writeImmutableAuthority(ctx, directoryFD, authority); err != nil {
		cleanup()
		return nil, err
	}
	log, err := openOrCreateLog(directoryFD)
	if err != nil {
		cleanup()
		return nil, err
	}
	raw, err := readLogBytes(log)
	if err != nil {
		_ = log.Close()
		cleanup()
		return nil, err
	}
	seed := initialChain(authority)
	state, tail, err := parseLog(raw, config, seed, true)
	if err != nil {
		_ = log.Close()
		cleanup()
		return nil, err
	}
	return &Journal{rootFD: rootFD, directoryFD: directoryFD, log: log,
		config: config, state: state, tail: tail,
		committedBytes: int64(len(raw) - len(tail))}, nil
}

func (journal *Journal) Close() error {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return nil
	}
	journal.closed = true
	return storage(errors.Join(journal.log.Close(), unix.Close(journal.directoryFD), unix.Close(journal.rootFD)))
}

func (journal *Journal) ReadState(ctx context.Context) (State, error) {
	if journal == nil {
		return State{}, invalid("Volume journal is unavailable")
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.available(ctx); err != nil {
		return State{}, err
	}
	return cloneState(journal.state), nil
}

func (journal *Journal) Intent(ctx context.Context, mutation backupvolumefs.Mutation) error {
	return journal.append(ctx, phaseIntent, mutation)
}

func (journal *Journal) Completed(ctx context.Context, mutation backupvolumefs.Mutation) error {
	return journal.append(ctx, phaseCompleted, mutation)
}

func (journal *Journal) append(ctx context.Context, phase byte, mutation backupvolumefs.Mutation) error {
	if journal == nil {
		return invalid("Volume journal is unavailable")
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.available(ctx); err != nil {
		return err
	}
	value := record{sequence: journal.state.RecordCount + 1, phase: phase,
		mutation: cloneMutation(mutation), prior: journal.state.ChainSHA256}
	raw, err := encodeRecord(value)
	if err != nil {
		return err
	}
	value, err = decodeRecord(raw)
	if err != nil {
		return err
	}
	candidate := journal.state
	if err := applyRecord(&candidate, journal.config, value, false); err != nil {
		return err
	}
	if len(journal.tail) > len(raw) || !bytes.Equal(journal.tail, raw[:len(journal.tail)]) {
		return conflict("Volume journal partial frame conflicts with exact replay")
	}
	if journal.committedBytes+int64(len(raw)) > MaximumJournalBytes {
		return invalid("Volume journal exceeds its capacity reservation")
	}
	info, err := journal.log.Stat()
	if err != nil {
		return storage(err)
	}
	if info.Size() != journal.committedBytes+int64(len(journal.tail)) {
		return conflict("Volume journal length changed outside its owner")
	}
	offset := journal.committedBytes + int64(len(journal.tail))
	written, writeErr := journal.log.WriteAt(raw[len(journal.tail):], offset)
	if writeErr == nil && written != len(raw)-len(journal.tail) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = journal.log.Sync()
	}
	if writeErr != nil {
		journal.poisoned = true
		return storage(writeErr)
	}
	candidate.RecordCount, candidate.ChainSHA256, candidate.UncommittedPrefix = value.sequence, value.hash, false
	if phase == phaseCompleted {
		candidate.Completed = append(candidate.Completed, cloneMutation(mutation))
	}
	journal.state = candidate
	journal.committedBytes += int64(len(raw))
	journal.tail = nil
	return nil
}

func (journal *Journal) available(ctx context.Context) error {
	if ctx == nil || journal == nil || journal.closed || journal.poisoned {
		return invalid("Volume journal is closed or requires recovery")
	}
	return ctx.Err()
}

func validateStageFilesystem(directoryFD int) error {
	stageFD, err := unix.Open(backupstage.AgentRoot,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return storage(err)
	}
	defer unix.Close(stageFD)
	var stage, journal unix.Stat_t
	if err := unix.Fstat(stageFD, &stage); err != nil {
		return storage(err)
	}
	if err := unix.Fstat(directoryFD, &journal); err != nil {
		return storage(err)
	}
	if stage.Dev != journal.Dev {
		return conflict("Volume journal and reserved staging filesystem differ")
	}
	return nil
}

func openAgentRoot(ctx context.Context, create bool) (int, bool, error) {
	current, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, false, storage(err)
	}
	parts := strings.Split(strings.TrimPrefix(AgentRoot, "/"), "/")
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
			if err := unix.Mkdirat(current, part, 0o700); err != nil {
				_ = unix.Close(current)
				return -1, false, storage(err)
			}
			if err := unix.Fsync(current); err != nil {
				_ = unix.Close(current)
				return -1, false, storage(err)
			}
			statErr = unix.Fstatat(current, part, &before, unix.AT_SYMLINK_NOFOLLOW)
		}
		if statErr != nil || before.Mode&unix.S_IFMT != unix.S_IFDIR || before.Uid != 0 ||
			(final && before.Mode&0o7777 != 0o700) || (!final && before.Mode&0o022 != 0) {
			_ = unix.Close(current)
			if statErr != nil {
				return -1, false, storage(statErr)
			}
			return -1, false, invalid("Volume journal root ancestry is unsafe")
		}
		resolve := uint64(unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS)
		if final {
			resolve = resolveLocal
		}
		next, err := unix.Openat2(current, part, &unix.OpenHow{
			Flags: uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: resolve,
		})
		_ = unix.Close(current)
		if err != nil {
			return -1, false, storage(err)
		}
		var after unix.Stat_t
		if err := unix.Fstat(next, &after); err != nil || after.Ino != before.Ino || after.Dev != before.Dev {
			_ = unix.Close(next)
			if err != nil {
				return -1, false, storage(err)
			}
			return -1, false, invalid("Volume journal root ancestor changed")
		}
		current = next
	}
	return current, true, nil
}

func openOrCreateDirectory(rootFD int, name string) (int, error) {
	var stat unix.Stat_t
	err := unix.Fstatat(rootFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		if err := unix.Mkdirat(rootFD, name, 0o700); err != nil {
			return -1, storage(err)
		}
		if err := unix.Fsync(rootFD); err != nil {
			return -1, storage(err)
		}
	} else if err != nil {
		return -1, storage(err)
	}
	return openExistingDirectory(rootFD, name)
}
