package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckpointBackupConfigCapture commits the capture-ready source edge with an
// exact compare on the native transfer cursor that proved completion. A later
// credit cannot race this publication, and the ordinary checkpoint writer has
// no path for this transition.
func (repository *BackupRuntimeRepository) CheckpointBackupConfigCapture(
	ctx context.Context,
	checkpoint backupruntime.BackupCheckpointInput,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
	cursor etcdstore.Versioned[backupconfiguration.ConfigTransferCursor],
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	if backupruntime.TerminalBackupRunState(next.State) ||
		backupruntime.ValidateBackupRunTransition(
			current.Record,
			next,
			backupruntime.BackupRunTransitionOrdinary,
		) != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"checkpointed Config capture transition is invalid",
		)
	}
	ordinal, changed := backupruntime.ChangedBackupSourceOrdinal(current.Record, next)
	if !changed || !backupruntime.BackupRunConfigCaptureCheckpointMatchesTransition(
		checkpoint.Request,
		current.Record.Sources[ordinal],
		next.Sources[ordinal],
	) || cursor.Revision <= 0 || cursor.ReadRevision < cursor.Revision ||
		cursor.Record.Owner.Binding.TaskID != current.Record.TaskID ||
		cursor.Record.Owner.Binding.StepID != checkpoint.StepID ||
		cursor.Record.Owner.Binding.ExecutionID != checkpoint.ExecutionID ||
		cursor.Record.Owner.Binding.AssignmentID != checkpoint.AssignmentID {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Config capture checkpoint does not match its native cursor",
		)
	}
	condition := etcdstore.Condition{
		Key:         backupconfiguration.ConfigTransferCursorKey(cursor.Record.Owner.Binding),
		ModRevision: cursor.Revision,
	}
	validate := func(values []*etcdstore.KeyValue) error {
		if len(values) != 1 || values[0] == nil || values[0].Key != condition.Key ||
			values[0].ModRevision != cursor.Revision {
			return errs.New(errs.KindStateConflict, "Config transfer cursor changed")
		}
		stored, err := backupconfiguration.DecodeConfigTransferCursor(values[0].Value)
		if err != nil || stored != cursor.Record {
			return errs.New(errs.KindStateConflict, "Config transfer cursor changed")
		}
		return nil
	}
	return repository.replaceBackupRun(
		ctx,
		current,
		next,
		[]etcdstore.Condition{condition},
		nil,
		validate,
		nil,
		&checkpoint,
	)
}
