package agentconfigjournal

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"golang.org/x/sys/unix"
)

// Cleanup requires the native Capture or Restore source-cleanup
// receipt, after physical staging has been removed. Until then the record/credit
// journal remains resume authority.
// This removes only the validated leaves in the still-locked transfer inode.
func (journal *Journal) Cleanup(ctx context.Context, completion *agentpb.BackupStepResume) error {
	if journal == nil {
		return invalidRecord()
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.available(ctx); err != nil {
		return err
	}
	if err := journal.recordCleanupReceipt(ctx, completion); err != nil {
		return err
	}
	return journal.cleanupLocked(ctx)
}

func (journal *Journal) cleanupLocked(ctx context.Context) error {
	directory, err := journal.root.Open(".")
	if err != nil {
		return fileError(err)
	}
	defer directory.Close()
	var retainedNames []string
	for {
		names, readErr := directory.Readdirnames(96)
		retainedNames = append(retainedNames, names...)
		if uint64(len(retainedNames)) > 2*journal.maxRecords+5 {
			return invalidRecord()
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fileError(readErr)
		}
	}
	for _, name := range retainedNames {
		if name == authorityName || name == cleanupReceiptName {
			continue // Remove the binding last, after all dependent receipts.
		}
		sequence, _, record := parseRecordName(name)
		creditSequence, _, credit := parseCreditName(name)
		if !(record && sequence > 0 && sequence <= journal.maxRecords) &&
			!(credit && creditSequence > 0 && creditSequence <= journal.maxRecords+1) {
			return invalidRecord()
		}
		file, err := journal.openLeaf(ctx, name, unix.O_RDONLY, 0, backupconfigtransfer.MaximumRecordDescriptorBytes)
		if err != nil {
			return err
		}
		if err := file.Close(); err != nil {
			return fileError(err)
		}
		if err := journal.root.Remove(name); err != nil {
			return fileError(err)
		}
	}
	if err := journal.directory.Sync(); err != nil {
		return fileError(err)
	}
	parent, err := os.OpenRoot(agentprotocol.StatePath + "/config-transfers")
	if err != nil {
		return fileError(err)
	}
	defer parent.Close()
	actual, err := parent.Lstat(journal.binding.TransferID)
	if err != nil {
		return fileError(err)
	}
	expected, err := journal.directory.Stat()
	if err != nil || !os.SameFile(actual, expected) {
		return conflict("Config journal cleanup namespace changed")
	}
	if err := journal.root.Remove(authorityName); err != nil {
		return fileError(err)
	}
	if err := journal.directory.Sync(); err != nil {
		return fileError(err)
	}
	if err := journal.root.Remove(cleanupReceiptName); err != nil {
		return fileError(err)
	}
	if err := journal.directory.Sync(); err != nil {
		return fileError(err)
	}
	if err := parent.Remove(journal.binding.TransferID); err != nil {
		return fileError(err)
	}
	parentDirectory, err := parent.Open(".")
	if err != nil {
		return fileError(err)
	}
	return fileError(errors.Join(parentDirectory.Sync(), parentDirectory.Close()))
}
