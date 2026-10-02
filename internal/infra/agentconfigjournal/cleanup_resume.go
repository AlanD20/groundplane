package agentconfigjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ResumeCleanup accepts an already-absent journal only after validating the
// native cleanup receipt. It never creates a directory merely to retire it.
// Existing journals still prove their durable transfer or partial retirement.
func ResumeCleanup(ctx context.Context, path string, binding backupconfigtransfer.Binding,
	step *agentpb.BackupStepAuthority, completion *agentpb.BackupStepResume,
) (resultErr error) {
	owned, err := executionplan.ValidateBackupStepAuthority(step)
	if err != nil {
		return err
	}
	if err := binding.Validate(); err != nil {
		return err
	}
	expected := owned.GetCapture().GetConfig().GetContent()
	if binding.Direction == agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE {
		expected = owned.GetRestore().GetConfig().GetExpectedArchive().GetContent()
	}
	if expected == nil {
		return invalidRecord()
	}
	verifier := &Journal{binding: binding, step: owned, expected: expected}
	if err := verifier.validateCleanupReceipt(completion); err != nil {
		return err
	}
	journal, err := openJournal(ctx, path, binding, owned, false)
	if err != nil || journal == nil {
		return err
	}
	defer func() {
		if closeErr := journal.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	return journal.Cleanup(ctx, completion)
}
