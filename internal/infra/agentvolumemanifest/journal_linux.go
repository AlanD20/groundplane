//go:build linux

package agentvolumemanifest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

type Journal struct {
	mu             sync.Mutex
	rootFD         int
	directoryFD    int
	log            *os.File
	config         Config
	state          receiverState
	tail           []byte
	committedBytes int64
	closed         bool
	poisoned       bool
}

// Open pins one transfer directory, locks it across the process lifetime, and
// durably commits the initial native credit before returning it to any caller.
func Open(ctx context.Context, config Config) (*Journal, error) {
	if ctx == nil {
		return nil, invalid("Volume manifest receiver context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	authority, err := encodeAuthority(config)
	if err != nil {
		return nil, err
	}
	rootFD, found, err := openRoot(ctx, true)
	if err != nil || !found {
		return nil, err
	}
	if err := validateStageFilesystem(rootFD); err != nil {
		_ = unix.Close(rootFD)
		return nil, err
	}
	var retiring unix.Stat_t
	if err := unix.Fstatat(rootFD, config.Binding.TransferID+".retiring", &retiring,
		unix.AT_SYMLINK_NOFOLLOW); err == nil {
		_ = unix.Close(rootFD)
		return nil, conflict("Volume manifest receiver is already retiring")
	} else if !errors.Is(err, unix.ENOENT) {
		_ = unix.Close(rootFD)
		return nil, storage(err)
	}
	fd, err := openDirectory(rootFD, config.Binding.TransferID, true)
	if err != nil {
		_ = unix.Close(rootFD)
		return nil, err
	}
	closeFDs := func() { _ = unix.Close(fd); _ = unix.Close(rootFD) }
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeFDs()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, conflict("Volume manifest receiver is already open")
		}
		return nil, storage(err)
	}
	names, err := directoryNames(fd)
	if err != nil {
		closeFDs()
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
			closeFDs()
			return nil, invalid("Volume manifest receiver contains an unexpected leaf")
		}
	}
	if hasLog && !hasAuthority {
		closeFDs()
		return nil, invalid("Volume manifest receiver log lacks sealed authority")
	}
	if err := writeAuthority(ctx, fd, authority); err != nil {
		closeFDs()
		return nil, err
	}
	log, err := openLeaf(fd, logName, unix.O_RDWR, !hasLog, MaximumJournalBytes)
	if err != nil {
		closeFDs()
		return nil, err
	}
	if !hasLog {
		if err := errors.Join(log.Sync(), unix.Fsync(fd)); err != nil {
			_ = log.Close()
			closeFDs()
			return nil, storage(err)
		}
	}
	raw, err := readLog(log)
	if err != nil {
		_ = log.Close()
		closeFDs()
		return nil, err
	}
	state, tail, err := parseLog(raw, config, authority)
	if err != nil {
		_ = log.Close()
		closeFDs()
		return nil, err
	}
	if err := log.Sync(); err != nil {
		_ = log.Close()
		closeFDs()
		return nil, storage(err)
	}
	journal := &Journal{rootFD: rootFD, directoryFD: fd, log: log, config: config,
		state: state, tail: tail, committedBytes: int64(len(raw) - len(tail))}
	if len(state.credits) == 0 {
		credit, err := backupvolumetransfer.InitialCredit(config.Binding)
		if err != nil {
			_ = journal.Close()
			return nil, err
		}
		encoded, err := encodeCredit(credit)
		if err != nil {
			_ = journal.Close()
			return nil, err
		}
		if err := journal.appendTransaction(ctx, transaction{sequence: 0, credit: encoded,
			prior: journal.state.logHash}); err != nil {
			_ = journal.Close()
			return nil, err
		}
		journal.state.credits = append(journal.state.credits, credit)
	}
	return journal, nil
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

func (journal *Journal) CurrentCredit(ctx context.Context) (*agentpb.BackupVolumeManifestAckCredit, error) {
	if journal == nil {
		return nil, invalid("Volume manifest receiver is unavailable")
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.available(ctx); err != nil {
		return nil, err
	}
	return proto.CloneOf(journal.state.credits[len(journal.state.credits)-1]), nil
}

// AcceptFrame persists exact frame bytes and the resulting cumulative credit
// as one hash-chained transaction, fsynced before the credit is exposed.
func (journal *Journal) AcceptFrame(ctx context.Context, frame *agentpb.BackupVolumeManifestTransfer) (
	*agentpb.BackupVolumeManifestAckCredit, error,
) {
	if journal == nil {
		return nil, invalid("Volume manifest receiver is unavailable")
	}
	owned, err := backupvolumetransfer.ValidateFrame(journal.config.Binding, frame)
	if err != nil {
		return nil, err
	}
	raw, err := encodeFrame(owned)
	if err != nil {
		return nil, err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.available(ctx); err != nil {
		return nil, err
	}
	sequence := owned.RecordSequence
	if sequence <= uint64(len(journal.state.frames)) {
		index := int(sequence - 1)
		if !bytes.Equal(journal.state.frames[index], raw) {
			return nil, conflict("Volume manifest receiver replay changes committed frame bytes")
		}
		if err := journal.log.Sync(); err != nil {
			journal.poisoned = true
			return nil, storage(err)
		}
		return proto.CloneOf(journal.state.credits[sequence]), nil
	}
	candidate, credit, err := journal.state.advance(journal.config, owned)
	if err != nil {
		return nil, err
	}
	encodedCredit, err := encodeCredit(credit)
	if err != nil {
		return nil, err
	}
	if err := journal.appendTransaction(ctx, transaction{sequence: sequence, frame: raw,
		credit: encodedCredit, prior: journal.state.logHash}); err != nil {
		return nil, err
	}
	candidate.logHash = journal.state.logHash
	candidate.frames = append(candidate.frames, raw)
	candidate.credits = append(candidate.credits, credit)
	candidate.partial = false
	journal.state = candidate
	return proto.CloneOf(credit), nil
}

func (journal *Journal) ReadComplete(ctx context.Context) ([]backupvolume.Entry,
	[]backupvolume.ManifestEntryBytes, error,
) {
	if journal == nil {
		return nil, nil, invalid("Volume manifest receiver is unavailable")
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.available(ctx); err != nil {
		return nil, nil, err
	}
	if !journal.state.complete || journal.state.partial {
		return nil, nil, conflict("Volume manifest receiver is not complete")
	}
	entries := make([]backupvolume.Entry, len(journal.state.entries))
	manifest := make([]backupvolume.ManifestEntryBytes, len(journal.state.manifest))
	for index := range entries {
		entries[index] = journal.state.entries[index]
		entries[index].Path = append([]byte(nil), entries[index].Path...)
		manifest[index] = journal.state.manifest[index]
		manifest[index].Bytes = append([]byte(nil), manifest[index].Bytes...)
	}
	return entries, manifest, nil
}

func (journal *Journal) appendTransaction(ctx context.Context, value transaction) error {
	if err := journal.available(ctx); err != nil {
		return err
	}
	raw, err := encodeTransaction(value)
	if err != nil {
		return err
	}
	decoded, err := decodeTransaction(raw)
	if err != nil {
		return err
	}
	if len(journal.tail) > len(raw) || !bytes.Equal(journal.tail, raw[:len(journal.tail)]) ||
		journal.committedBytes+int64(len(raw)) > MaximumJournalBytes {
		return conflict("Volume manifest receiver partial transaction or capacity conflicts")
	}
	info, err := journal.log.Stat()
	if err != nil {
		return storage(err)
	}
	if info.Size() != journal.committedBytes+int64(len(journal.tail)) {
		return conflict("Volume manifest receiver log changed outside its owner")
	}
	remaining := raw[len(journal.tail):]
	written, writeErr := journal.log.WriteAt(remaining, journal.committedBytes+int64(len(journal.tail)))
	if writeErr == nil && written != len(remaining) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = journal.log.Sync()
	}
	if writeErr != nil {
		journal.poisoned = true
		return storage(writeErr)
	}
	journal.committedBytes += int64(len(raw))
	journal.tail = nil
	journal.state.logHash = decoded.hash
	journal.state.partial = false
	return nil
}

func (journal *Journal) available(ctx context.Context) error {
	if ctx == nil || journal == nil || journal.closed || journal.poisoned {
		return invalid("Volume manifest receiver is closed or requires recovery")
	}
	return ctx.Err()
}

func readLog(file *os.File) ([]byte, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, storage(err)
	}
	if info.Size() < 0 || info.Size() > MaximumJournalBytes {
		return nil, invalid("Volume manifest receiver log exceeds its bound")
	}
	raw, err := io.ReadAll(io.NewSectionReader(file, 0, info.Size()))
	if err != nil {
		return nil, storage(err)
	}
	if int64(len(raw)) != info.Size() {
		return nil, invalid("Volume manifest receiver log changed during replay")
	}
	return raw, nil
}
