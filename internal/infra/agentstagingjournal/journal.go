package agentstagingjournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const (
	recordName = "recovery.json"
	nextName   = "recovery.next"
)

type Journal struct {
	mu              sync.Mutex
	root            *os.Root
	directory       *os.File
	closed          bool
	acceptedReceipt *agentpb.BackupStagingRecoveryAckReceipt
}

// Open requires a pre-created dedicated private journal directory. It neither
// creates staging roots nor follows directory symlinks during traversal.
func Open(path string) (*Journal, error) {
	base := agentprotocol.StatePath + "/staging"
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || (path != base && !strings.HasPrefix(path, base+"/")) {
		return nil, invalidRecord()
	}
	root, err := os.OpenRoot("/")
	if err != nil {
		return nil, fileError(err)
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for index, part := range parts {
		before, err := root.Lstat(part)
		if errors.Is(err, fs.ErrNotExist) && index == len(parts)-1 {
			if createErr := root.Mkdir(part, 0o700); createErr != nil && !errors.Is(createErr, fs.ErrExist) {
				_ = root.Close()
				return nil, fileError(createErr)
			}
			parent, openErr := root.Open(".")
			if openErr != nil {
				_ = root.Close()
				return nil, fileError(openErr)
			}
			syncErr := errors.Join(parent.Sync(), parent.Close())
			if syncErr != nil {
				_ = root.Close()
				return nil, fileError(syncErr)
			}
			before, err = root.Lstat(part)
		}
		if err != nil {
			_ = root.Close()
			return nil, fileError(err)
		} // Cleanup after failed traversal.
		stat, ok := before.Sys().(*syscall.Stat_t)
		if !ok || !before.IsDir() || stat.Uid != 0 || stat.Mode&0o022 != 0 {
			_ = root.Close()
			return nil, invalidRecord()
		} // Reject replaceable ancestors.
		next, err := root.OpenRoot(part)
		if err != nil {
			_ = root.Close()
			return nil, fileError(err)
		} // Cleanup after failed directory pin.
		after, statErr := next.Stat(".")
		closeErr := root.Close()
		if statErr != nil || closeErr != nil || !os.SameFile(before, after) {
			_ = next.Close()
			return nil, invalidRecord()
		} // Reject a raced directory.
		root = next
	}
	directory, err := root.Open(".")
	if err != nil {
		_ = root.Close()
		return nil, fileError(err)
	} // Cleanup after failed root descriptor.
	info, err := directory.Stat()
	if err == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.IsDir() || stat.Uid != 0 || stat.Mode&0o7777 != 0o700 {
			err = invalidRecord()
		}
	}
	if err != nil {
		_ = directory.Close()
		_ = root.Close()
		return nil, fileError(err)
	} // Reject an unsafe journal root.
	return &Journal{root: root, directory: directory}, nil
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

// Read exposes retained prior-process authority for Controller reconciliation.
// Absence is only absence of this journal, never physical stage discard proof.
func (journal *Journal) Read(ctx context.Context) (Record, bool, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return Record{}, false, err
	}
	defer unlock()
	return journal.read(ctx)
}

func (journal *Journal) RecordPlan(ctx context.Context, processGeneration [16]byte,
	inventory *agentpb.BackupStagingInventory, plan *agentpb.BackupStagingRecoveryPlan,
) error {
	proposed := Record{ProcessGeneration: processGeneration, Inventory: inventory, Plan: plan, Phase: PhasePlanRecorded}
	if err := validateRecord(proposed); err != nil {
		return err
	}
	proposed.Inventory = proto.Clone(inventory).(*agentpb.BackupStagingInventory)
	proposed.Plan = proto.Clone(plan).(*agentpb.BackupStagingRecoveryPlan)
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	current, found, err := journal.read(ctx)
	if err != nil {
		return err
	}
	if found {
		if !proto.Equal(current.Inventory, proposed.Inventory) {
			return conflict()
		}
		if !proto.Equal(current.Plan, proposed.Plan) {
			previous, err := executionplan.BackupStagingRecoveryPlanSHA256(current.Plan)
			if err != nil || current.Phase == PhaseReceiptAccepted ||
				!bytes.Equal(previous, proposed.Plan.SupersedesPlanSha256) {
				return conflict()
			}
			// The Controller has reclassified the same retained inventory under
			// new native authority. Old Acks cannot acknowledge this new plan.
			return journal.write(ctx, proposed)
		}
		if current.ProcessGeneration != proposed.ProcessGeneration {
			if current.Phase == PhaseReceiptAccepted {
				return conflict()
			}
			// The caller has received this same persisted plan under its current
			// authenticated process. Preserve applied work; only delivery rebinds.
			current.ProcessGeneration = proposed.ProcessGeneration
			return journal.write(ctx, current)
		}
		return fileError(journal.directory.Sync()) // Exact replay never rewinds its phase.
	}
	return journal.write(ctx, proposed)
}

func (journal *Journal) MarkApplied(ctx context.Context, ack *agentpb.BackupStagingRecoveryAck) error {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || len(ack.InventorySha256) != sha256.Size ||
		len(ack.AppliedPlanSha256) != sha256.Size {
		return invalidRecord()
	}
	owned := proto.Clone(ack).(*agentpb.BackupStagingRecoveryAck)
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	record, found, err := journal.read(ctx)
	if err != nil {
		return err
	}
	if !found {
		return conflict()
	}
	if err := ackMatchesPlan(owned, record); err != nil {
		return err
	}
	if record.Phase == PhaseApplied && proto.Equal(record.Ack, owned) {
		return fileError(journal.directory.Sync())
	}
	if record.Phase != PhasePlanRecorded {
		return conflict()
	}
	record.Phase, record.Ack = PhaseApplied, owned
	return journal.write(ctx, record)
}

// AcceptReceipt durably records exact Controller acceptance before bounded
// journal cleanup. It never removes or mutates inventoried physical artifacts.
func (journal *Journal) AcceptReceipt(ctx context.Context, receipt *agentpb.BackupStagingRecoveryAckReceipt) error {
	if err := validateReceipt(receipt); err != nil {
		return err
	}
	owned := proto.Clone(receipt).(*agentpb.BackupStagingRecoveryAckReceipt)
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	record, found, err := journal.read(ctx)
	if err != nil {
		return err
	}
	if !found {
		if journal.acceptedReceipt != nil && proto.Equal(journal.acceptedReceipt, owned) {
			return fileError(journal.directory.Sync())
		}
		return conflict()
	}
	if record.Phase != PhaseApplied && record.Phase != PhaseReceiptAccepted {
		return conflict()
	}
	if err := receiptMatchesRecord(owned, record); err != nil {
		return err
	}
	if record.Receipt != nil && !proto.Equal(record.Receipt, owned) {
		return conflict()
	}
	if record.Phase != PhaseReceiptAccepted {
		record.Phase, record.Receipt = PhaseReceiptAccepted, owned
		if err := journal.write(ctx, record); err != nil {
			return err
		}
	}
	if err := journal.directory.Sync(); err != nil {
		return fileError(err)
	} // Repair an uncertain earlier acceptance rename before cleanup.
	journal.acceptedReceipt = owned
	if err := ctx.Err(); err != nil {
		return err
	} // Retain accepted authority until exact cleanup can finish.
	if err := journal.removeNext(ctx); err != nil {
		return err
	}
	if err := journal.root.Remove(recordName); err != nil {
		return fileError(err)
	}
	return fileError(journal.directory.Sync())
}

func (journal *Journal) read(ctx context.Context) (Record, bool, error) {
	directory, err := journal.root.Open(".")
	if err != nil {
		return Record{}, false, fileError(err)
	}
	names, readErr := directory.Readdirnames(3)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return Record{}, false, fileError(readErr)
	}
	if closeErr != nil {
		return Record{}, false, fileError(closeErr)
	}
	if len(names) > 2 {
		return Record{}, false, invalidRecord()
	}
	for _, name := range names {
		if name != recordName && name != nextName {
			return Record{}, false, invalidRecord()
		}
		if name == nextName {
			file, err := journal.openLeaf(ctx, name, unix.O_RDONLY, 0)
			if err != nil {
				return Record{}, false, err
			}
			if err := file.Close(); err != nil {
				return Record{}, false, fileError(err)
			}
		}
	}
	file, err := journal.openLeaf(ctx, recordName, unix.O_RDONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, MaximumRecordBytes+1))
	closeErr = file.Close()
	if readErr != nil || closeErr != nil {
		return Record{}, false, fileError(errors.Join(readErr, closeErr))
	}
	record, err := decodeRecord(raw)
	if err != nil {
		return Record{}, false, err
	}
	return record, true, nil
}

func (journal *Journal) write(ctx context.Context, record Record) error {
	raw, err := encodeRecord(record)
	if err != nil {
		return err
	}
	if err := journal.removeNext(ctx); err != nil {
		return err
	}
	file, err := journal.openLeaf(ctx, nextName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, 0o600)
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
	} // Leave an explicitly uncommitted .next leaf.
	if err := journal.root.Rename(nextName, recordName); err != nil {
		return fileError(err)
	}
	return fileError(journal.directory.Sync())
}

func (journal *Journal) removeNext(ctx context.Context) error {
	file, err := journal.openLeaf(ctx, nextName, unix.O_RDONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return fileError(err)
	}
	return fileError(journal.root.Remove(nextName))
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
		if !ok || !info.Mode().IsRegular() || stat.Uid != 0 || stat.Nlink != 1 || stat.Mode&0o7777 != 0o600 ||
			info.Size() > MaximumRecordBytes {
			err = invalidRecord()
		}
	}
	if err != nil {
		_ = file.Close()
		return nil, fileError(err)
	} // Reject unsafe descriptors before I/O.
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
		return nil, errs.New(errs.KindStateConflict, "Agent staging journal is closed")
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
				) // Cleanup only releases this operation's advisory lock.
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
