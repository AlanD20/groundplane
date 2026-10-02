package agentterminaljournal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

type Journal struct {
	mu        sync.Mutex
	root      *os.Root
	directory *os.File
	uid       uint32
	closed    bool
}

// Open creates only the supplied leaf, never its parents, and pins its identity.
func Open(path string) (*Journal, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return nil, invalidRecord()
	}
	created := false
	if err := os.Mkdir(path, 0o700); err == nil {
		created = true
	} else if !errors.Is(err, fs.ErrExist) {
		return nil, fileError(err)
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, fileError(err)
	}
	if !before.IsDir() {
		return nil, invalidRecord()
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, fileError(err)
	}
	directory, err := root.Open(".")
	if err != nil {
		_ = root.Close()
		return nil, fileError(err)
	} // Cleanup after failed root pin.
	after, err := directory.Stat()
	if err != nil || !os.SameFile(before, after) || !privateDirectory(after, uint32(os.Geteuid())) {
		_ = directory.Close()
		_ = root.Close() // Reject a raced or unsafe root.
		return nil, invalidRecord()
	}
	if created {
		parent, openErr := os.OpenFile(
			filepath.Dir(path),
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
			0,
		)
		if openErr == nil {
			openErr = errors.Join(parent.Sync(), parent.Close())
		}
		if openErr != nil {
			_ = directory.Close()
			_ = root.Close()
			return nil, fileError(openErr)
		} // Cleanup after failed creation durability.
	}
	return &Journal{root: root, directory: directory, uid: uint32(os.Geteuid())}, nil
}

func privateDirectory(info fs.FileInfo, uid uint32) bool {
	if info == nil {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.IsDir() && stat.Uid == uid && stat.Mode&0o7777 == 0o700
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
	return fileError(errors.Join(journal.directory.Close(), journal.root.Close()))
}

// List validates every committed leaf and recognizes only exact uncommitted
// Task .next leaves. It never silently discards or classifies committed state.
func (journal *Journal) List(ctx context.Context) ([]Record, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return journal.list(ctx)
}

func (journal *Journal) list(ctx context.Context) ([]Record, error) {
	directory, err := journal.root.Open(".")
	if err != nil {
		return nil, fileError(err)
	}
	defer directory.Close() // Read-only enumeration cleanup.
	var records []Record
	entries := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		names, readErr := directory.Readdirnames(96)
		for _, name := range names {
			entries++
			if entries > 2*MaximumRecords {
				return nil, invalidRecord()
			}
			suffix := ".json"
			if strings.HasSuffix(name, ".next") {
				suffix = ".next"
			}
			taskID := strings.TrimSuffix(name, suffix)
			if taskID == name || ids.Validate(ids.KindTask, taskID) != nil {
				return nil, invalidRecord()
			}
			file, err := journal.openLeaf(ctx, name, unix.O_RDONLY, 0)
			if err != nil {
				return nil, err
			}
			if suffix == ".next" {
				if err := file.Close(); err != nil {
					return nil, fileError(err)
				}
				continue
			}
			raw, err := io.ReadAll(io.LimitReader(file, MaximumRecordBytes+1))
			closeErr := file.Close()
			if err != nil || closeErr != nil {
				return nil, fileError(errors.Join(err, closeErr))
			}
			record, err := decodeRecord(raw)
			if err != nil {
				return nil, err
			}
			if record.Ack.TaskId != taskID || len(records) >= MaximumRecords {
				return nil, invalidRecord()
			}
			records = append(records, record)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fileError(readErr)
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Ack.TaskId < records[j].Ack.TaskId })
	return records, nil
}

func (journal *Journal) PutPending(ctx context.Context, ack *agentpb.TaskAck) error {
	if err := validateRecord(Record{Ack: ack, Phase: PhasePending}); err != nil {
		return err
	}
	owned := proto.Clone(ack).(*agentpb.TaskAck)
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	records, err := journal.list(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Ack.TaskId == owned.TaskId {
			if proto.Equal(record.Ack, owned) {
				return fileError(journal.directory.Sync())
			}
			return conflict()
		}
	}
	if len(records) >= MaximumRecords {
		return invalidRecord()
	}
	return journal.write(ctx, Record{Ack: owned, Phase: PhasePending})
}

func (journal *Journal) Apply(ctx context.Context, receipt *agentpb.TaskTerminalReceiptAck) error {
	return journal.advance(ctx, receipt, PhasePending, PhaseApplied)
}

func (journal *Journal) Retire(ctx context.Context, receipt *agentpb.TaskTerminalReceiptAck) error {
	return journal.advance(ctx, receipt, PhaseApplied, PhaseRetired)
}

func (journal *Journal) advance(ctx context.Context, receipt *agentpb.TaskTerminalReceiptAck, from, to Phase) error {
	if err := executionplan.ValidateTaskTerminalReceiptAck(receipt); err != nil {
		return err
	}
	owned := proto.Clone(receipt).(*agentpb.TaskTerminalReceiptAck)
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	record, found, err := journal.read(ctx, owned.TaskId)
	if err != nil {
		return err
	}
	if !found {
		return conflict()
	}
	if err := receiptMatchesAck(owned, record.Ack); err != nil {
		return err
	}
	if record.Receipt != nil && !proto.Equal(record.Receipt, owned) {
		if to != PhaseApplied || !executionplan.SameTaskTerminalReceiptAuthority(record.Receipt, owned) ||
			bytes.Equal(record.Receipt.ProcessGeneration, owned.ProcessGeneration) {
			return conflict()
		}
		// Only receipt application may bind the same immutable result to the
		// replacement process after its Controller confirms the retained Task.
		record.Phase, record.Receipt = PhaseApplied, owned
		return journal.write(ctx, record)
	}
	if record.Phase == PhaseRetired && to == PhaseApplied {
		return fileError(journal.directory.Sync())
	}
	if record.Phase == to {
		return fileError(journal.directory.Sync())
	}
	if record.Phase != from {
		return conflict()
	}
	record.Phase, record.Receipt = to, owned
	return journal.write(ctx, record)
}

// RemoveRetired accepts only the exact receipt already journaled as retired.
// Routing calls it after the Controller acknowledges assignment retirement.
func (journal *Journal) RemoveRetired(ctx context.Context, receipt *agentpb.TaskTerminalReceiptAck) error {
	if err := executionplan.ValidateTaskTerminalReceiptAck(receipt); err != nil {
		return err
	}
	owned := proto.Clone(receipt).(*agentpb.TaskTerminalReceiptAck)
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	record, found, err := journal.read(ctx, owned.TaskId)
	if err != nil {
		return err
	}
	if !found {
		return fileError(journal.directory.Sync())
	} // Exact deletion replay has no remaining mutation.
	if record.Phase != PhaseRetired || !proto.Equal(record.Receipt, owned) {
		return conflict()
	}
	if err := journal.root.Remove(owned.TaskId + ".json"); err != nil {
		return fileError(err)
	}
	return fileError(journal.directory.Sync())
}

func (journal *Journal) read(ctx context.Context, taskID string) (Record, bool, error) {
	file, err := journal.openLeaf(ctx, taskID+".json", unix.O_RDONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, MaximumRecordBytes+1))
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		return Record{}, false, fileError(errors.Join(err, closeErr))
	}
	record, err := decodeRecord(raw)
	if err != nil {
		return Record{}, false, err
	}
	if record.Ack.TaskId != taskID {
		return Record{}, false, invalidRecord()
	}
	return record, true, nil
}

func (journal *Journal) write(ctx context.Context, record Record) error {
	raw, err := encodeRecord(record)
	if err != nil {
		return err
	}
	name := record.Ack.TaskId + ".next"
	if existing, err := journal.openLeaf(ctx, name, unix.O_RDONLY, 0); err == nil {
		if err := existing.Close(); err != nil {
			return fileError(err)
		}
		if err := journal.root.Remove(name); err != nil {
			return fileError(err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	file, err := journal.openLeaf(ctx, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(raw)
	if writeErr == nil && written != len(raw) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return fileError(errors.Join(writeErr, closeErr))
	}
	if err := ctx.Err(); err != nil {
		return err
	} // An uncommitted .next remains recoverable.
	if err := journal.root.Rename(name, record.Ack.TaskId+".json"); err != nil {
		return fileError(err)
	}
	return fileError(journal.directory.Sync())
}

func (journal *Journal) openLeaf(ctx context.Context, name string, flags int, mode uint32) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fd, err := unix.Openat(
		int(journal.directory.Fd()),
		name,
		flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC,
		mode,
	)
	if err != nil {
		return nil, fileError(&os.PathError{Op: "openat", Path: name, Err: err})
	}
	file := os.NewFile(uintptr(fd), name)
	info, err := file.Stat()
	if err == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.Mode().IsRegular() || stat.Mode&0o7777 != 0o600 || stat.Uid != journal.uid || stat.Nlink != 1 ||
			info.Size() > MaximumRecordBytes {
			err = invalidRecord()
		}
	}
	if err != nil {
		_ = file.Close()
		return nil, fileError(err)
	} // Reject an unsafe descriptor before I/O.
	return file, nil
}

func (journal *Journal) lock(ctx context.Context) (func(), error) {
	if journal == nil || ctx == nil {
		return nil, invalidRecord()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	journal.mu.Lock()
	if journal.closed {
		journal.mu.Unlock()
		return nil, errs.New(errs.KindStateConflict, "Agent terminal journal is closed")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := unix.Flock(int(journal.directory.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return func() {
				_ = unix.Flock(
					int(journal.directory.Fd()),
					unix.LOCK_UN,
				) // Cleanup releases only this operation's advisory lock.
				journal.mu.Unlock()
			}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EINTR) {
			journal.mu.Unlock()
			return nil, fileError(err)
		}
		select {
		case <-ctx.Done():
			journal.mu.Unlock()
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}
