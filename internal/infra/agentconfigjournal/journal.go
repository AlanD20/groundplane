// Package agentconfigjournal owns the private crash-durable Config transfer
// record journal. Selected-value plaintext remains exclusively in staging.
package agentconfigjournal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const (
	authorityName       = "authority.bin"
	authorityNextName   = "authority.next"
	authoritySchema     = byte(1)
	committedSuffix     = ".record"
	uncommittedSuffix   = ".next"
	sequenceDigits      = 20
	maximumAuthorityLen = backupconfigtransfer.MaximumJournalAuthorityBytes
)

type Journal struct {
	mu                  sync.Mutex
	root                *os.Root
	directory           *os.File
	binding             backupconfigtransfer.Binding
	expected            *agentpb.BackupConfigContentAuthority
	step                *agentpb.BackupStepAuthority
	maxRecords          uint64
	recordCount         uint64
	creditCount         uint64
	lastCreditCommitted uint64
	metadataAccepted    bool
	transfer            transferState
	closed              bool
	retired             bool
}

// Open pins one transfer-specific directory below the Agent Config transfer
// root. It holds a process-exclusive lock until Close.
func Open(
	ctx context.Context,
	path string,
	binding backupconfigtransfer.Binding,
	step *agentpb.BackupStepAuthority,
) (*Journal, error) {
	return openJournal(ctx, path, binding, step, true)
}

func openJournal(ctx context.Context, path string, binding backupconfigtransfer.Binding,
	step *agentpb.BackupStepAuthority, create bool,
) (*Journal, error) {
	if ctx == nil {
		return nil, invalidRecord()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ownedStep, err := executionplan.ValidateBackupStepAuthority(step)
	if err != nil {
		return nil, err
	}
	var expected *agentpb.BackupConfigContentAuthority
	switch binding.Direction {
	case agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE:
		expected = ownedStep.GetCapture().GetConfig().GetContent()
	case agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE:
		expected = ownedStep.GetRestore().GetConfig().GetExpectedArchive().GetContent()
	}
	base := agentprotocol.StatePath + "/config-transfers"
	if err := binding.Validate(); err != nil ||
		ownedStep.StepId != binding.StepID || ownedStep.ExecutionId != binding.ExecutionID ||
		!validExpected(expected) ||
		!filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != base ||
		filepath.Base(path) != binding.TransferID {
		return nil, invalidRecord()
	}
	root, directory, err := openPrivateRoot(ctx, path, base, create)
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, nil
	}
	cleanup := func() { _ = errors.Join(directory.Close(), root.Close()) }
	if err := validateJournalFilesystem(ctx, directory); err != nil {
		cleanup()
		return nil, err
	}
	if err := unix.Flock(int(directory.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		cleanup()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, conflict("Agent Config journal is already open")
		}
		return nil, fileError(err)
	}
	maximum, err := backupconfigtransfer.MaximumTransferRecords(expected)
	if err != nil {
		cleanup()
		return nil, err
	}
	journal := &Journal{
		root: root, directory: directory, binding: binding, expected: proto.CloneOf(expected), step: ownedStep,
		maxRecords: maximum,
	}
	if err := journal.pinAuthority(ctx); err != nil {
		_ = unix.Flock(int(directory.Fd()), unix.LOCK_UN)
		cleanup()
		return nil, err
	}
	_, cleanupFound, err := journal.readCleanupReceipt(ctx)
	if err != nil {
		_ = journal.Close()
		return nil, err
	}
	if cleanupFound {
		journal.retired = true
		return journal, nil
	}
	recordCount, creditCount, err := journal.scan(ctx)
	if err != nil {
		_ = unix.Flock(int(directory.Fd()), unix.LOCK_UN)
		cleanup()
		return nil, err
	}
	journal.recordCount, journal.creditCount = recordCount, creditCount
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
	return fileError(errors.Join(
		unix.Flock(int(journal.directory.Fd()), unix.LOCK_UN),
		journal.directory.Close(),
		journal.root.Close(),
	))
}

// AppendRecord durably commits exactly the next descriptor. Replaying the
// identical committed sequence is allowed; changing it is rejected.
func (journal *Journal) AppendRecord(
	ctx context.Context,
	descriptor *backupconfigtransfer.RecordDescriptor,
) error {
	if journal == nil {
		return invalidRecord()
	}
	raw, err := backupconfigtransfer.MarshalRecordDescriptor(journal.binding, descriptor)
	if err != nil {
		return err
	}
	if err := journal.descriptorMatchesAuthority(descriptor); err != nil {
		return err
	}
	sequence := descriptor.Frame.RecordSequence
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.available(ctx); err != nil {
		return err
	}
	if sequence == 0 || sequence > journal.maxRecords {
		return invalidRecord()
	}
	if journal.retired {
		return conflict("Config journal has already reached native cleanup")
	}
	if sequence <= journal.recordCount {
		current, err := journal.readRaw(ctx, recordName(sequence))
		if err != nil {
			return err
		}
		if !bytes.Equal(current, raw) {
			return conflict("Agent Config journal sequence conflicts")
		}
		return fileError(journal.directory.Sync())
	}
	if sequence != journal.recordCount+1 {
		return conflict("Agent Config journal sequence is not contiguous")
	}
	candidate := journal.transfer
	if descriptor.Frame.GetEntryHeader() != nil {
		candidate.entries = append([]transferEntry(nil), candidate.entries...)
	}
	if err := candidate.accept(journal.binding.Direction, journal.expected, descriptor); err != nil {
		return err
	}
	if err := journal.writeImmutable(ctx, recordName(sequence), nextName(sequence), raw,
		backupconfigtransfer.MaximumRecordDescriptorBytes); err != nil {
		return err
	}
	journal.recordCount = sequence
	journal.transfer = candidate
	return nil
}

// ReadRecord implements backupconfigtransfer.RecoveryRecordSource.
func (journal *Journal) ReadRecord(
	ctx context.Context,
	sequence uint64,
) (*backupconfigtransfer.RecordDescriptor, error) {
	if journal == nil {
		return nil, invalidRecord()
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.available(ctx); err != nil {
		return nil, err
	}
	if sequence == 0 || sequence > journal.recordCount {
		return nil, conflict("Agent Config journal record is unavailable")
	}
	raw, err := journal.readRaw(ctx, recordName(sequence))
	if err != nil {
		return nil, err
	}
	descriptor, err := backupconfigtransfer.UnmarshalRecordDescriptor(journal.binding, raw)
	if err != nil {
		return nil, err
	}
	if descriptor.Frame.RecordSequence != sequence {
		return nil, invalidRecord()
	}
	return descriptor, journal.descriptorMatchesAuthority(descriptor)
}

func (journal *Journal) available(ctx context.Context) error {
	if journal == nil || ctx == nil {
		return invalidRecord()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if journal.closed {
		return conflict("Agent Config journal is closed")
	}
	return nil
}

func (journal *Journal) descriptorMatchesAuthority(
	descriptor *backupconfigtransfer.RecordDescriptor,
) error {
	if start := descriptor.Frame.GetStart(); start != nil && !proto.Equal(start.Content, journal.expected) {
		return conflict("Agent Config journal start authority conflicts")
	}
	if end := descriptor.Frame.GetEnd(); end != nil && !proto.Equal(end.Content, journal.expected) {
		return conflict("Agent Config journal end authority conflicts")
	}
	return nil
}

func (journal *Journal) pinAuthority(ctx context.Context) error {
	want, err := encodeAuthority(journal.binding, journal.step)
	if err != nil {
		return err
	}
	current, err := journal.readRawLimit(ctx, authorityName, maximumAuthorityLen)
	if err == nil {
		if !bytes.Equal(current, want) {
			return conflict("Agent Config journal authority conflicts")
		}
		return nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return journal.writeImmutable(ctx, authorityName, authorityNextName, want, maximumAuthorityLen)
}

func (journal *Journal) scan(ctx context.Context) (uint64, uint64, error) {
	directory, err := journal.root.Open(".")
	if err != nil {
		return 0, 0, fileError(err)
	}
	defer directory.Close()
	seen := make([]bool, journal.maxRecords+1)
	records := make([]*backupconfigtransfer.RecordDescriptor, journal.maxRecords+1)
	seenCredits := make([]bool, journal.maxRecords+2)
	var count uint64
	var creditCount uint64
	var pendingRecord, pendingCredit uint64
	for {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		names, readErr := directory.Readdirnames(96)
		for _, name := range names {
			if name == authorityName {
				continue
			}
			if name == cleanupReceiptNextName {
				file, err := journal.openLeaf(ctx, name, unix.O_RDONLY, 0, maximumAuthorityLen)
				if err != nil {
					return 0, 0, err
				}
				if err := file.Close(); err != nil {
					return 0, 0, fileError(err)
				}
				continue
			}
			if name == authorityNextName {
				return 0, 0, conflict("Agent Config journal has uncertain authority")
			}
			if sequence, committed, ok := parseCreditName(name); ok {
				if sequence == 0 || sequence > journal.maxRecords+1 || seenCredits[sequence] {
					return 0, 0, invalidRecord()
				}
				if !committed {
					if pendingCredit != 0 {
						return 0, 0, invalidRecord()
					}
					file, err := journal.openLeaf(ctx, name, unix.O_RDONLY, 0, maximumCreditBytes)
					if err != nil {
						return 0, 0, err
					}
					if err := file.Close(); err != nil {
						return 0, 0, fileError(err)
					}
					pendingCredit = sequence
					continue
				}
				credit, err := journal.readCredit(ctx, name)
				if err != nil || credit.CreditSequence != sequence {
					return 0, 0, invalidRecord()
				}
				seenCredits[sequence], creditCount = true, creditCount+1
				continue
			}
			sequence, committed, ok := parseRecordName(name)
			if !ok || sequence == 0 || sequence > journal.maxRecords || seen[sequence] {
				return 0, 0, invalidRecord()
			}
			if !committed {
				if pendingRecord != 0 {
					return 0, 0, invalidRecord()
				}
				file, err := journal.openLeaf(
					ctx,
					name,
					unix.O_RDONLY,
					0,
					backupconfigtransfer.MaximumRecordDescriptorBytes,
				)
				if err != nil {
					return 0, 0, err
				}
				if err := file.Close(); err != nil {
					return 0, 0, fileError(err)
				}
				pendingRecord = sequence
				continue
			}
			raw, err := journal.readRaw(ctx, name)
			if err != nil {
				return 0, 0, err
			}
			descriptor, err := backupconfigtransfer.UnmarshalRecordDescriptor(journal.binding, raw)
			if err != nil || descriptor.Frame.RecordSequence != sequence {
				return 0, 0, invalidRecord()
			}
			if err := journal.descriptorMatchesAuthority(descriptor); err != nil {
				return 0, 0, err
			}
			seen[sequence] = true
			records[sequence] = descriptor
			count++
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return 0, 0, fileError(readErr)
		}
	}
	if (pendingRecord != 0 && pendingRecord != count+1) ||
		(pendingCredit != 0 && pendingCredit != creditCount+1) ||
		(pendingCredit != 0 && pendingRecord != 0) {
		return 0, 0, invalidRecord()
	}
	for sequence := uint64(1); sequence <= count; sequence++ {
		if !seen[sequence] {
			return 0, 0, invalidRecord()
		}
		if err := journal.transfer.accept(journal.binding.Direction, journal.expected, records[sequence]); err != nil {
			return 0, 0, err
		}
	}
	for sequence := uint64(1); sequence <= creditCount; sequence++ {
		if !seenCredits[sequence] {
			return 0, 0, invalidRecord()
		}
	}
	credits, err := journal.readCredits(ctx, creditCount)
	if err != nil {
		return 0, 0, err
	}
	if err := validateCreditHistory(credits, count); err != nil {
		return 0, 0, err
	}
	if len(credits) != 0 {
		journal.lastCreditCommitted = credits[len(credits)-1].CommittedRecordSequence
		for _, credit := range credits {
			if credit.GetMetadataAccepted() != nil {
				journal.metadataAccepted = true
			}
		}
	}
	return count, creditCount, nil
}

func (journal *Journal) readRaw(ctx context.Context, name string) ([]byte, error) {
	return journal.readRawLimit(ctx, name, backupconfigtransfer.MaximumRecordDescriptorBytes)
}

func (journal *Journal) readRawLimit(ctx context.Context, name string, limit int) ([]byte, error) {
	file, err := journal.openLeaf(ctx, name, unix.O_RDONLY, 0, limit)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, fileError(errors.Join(readErr, closeErr))
	}
	if len(raw) == 0 || len(raw) > limit {
		return nil, invalidRecord()
	}
	return raw, nil
}
