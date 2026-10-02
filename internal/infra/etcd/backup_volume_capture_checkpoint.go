package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckpointBackupVolumeCapture atomically binds selected artifact evidence to
// the completed native manifest cursor. An ordinary run write cannot publish
// this edge, and any intervening cursor change defeats its compare.
func (repository *BackupRuntimeRepository) CheckpointBackupVolumeCapture(
	ctx context.Context,
	checkpoint backupruntime.BackupCheckpointInput,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
	cursor etcdstore.Versioned[backupvolumemanifest.Cursor],
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	if backupruntime.TerminalBackupRunState(next.State) ||
		backupruntime.ValidateBackupRunTransition(
			current.Record,
			next,
			backupruntime.BackupRunTransitionOrdinary,
		) != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Volume capture transition is invalid",
		)
	}
	ordinal, changed := backupruntime.ChangedBackupSourceOrdinal(current.Record, next)
	if !changed || !backupruntime.BackupRunVolumeCaptureCheckpointMatchesTransition(
		checkpoint.Request, current.Record.Sources[ordinal], next.Sources[ordinal],
	) || cursor.Revision <= 0 || cursor.ReadRevision < cursor.Revision || !cursor.Record.Complete ||
		cursor.Record.Owner.Binding.TaskID != current.Record.TaskID ||
		cursor.Record.Owner.Binding.StepID != checkpoint.StepID ||
		cursor.Record.Owner.Binding.AssignmentID != checkpoint.AssignmentID {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Volume capture checkpoint does not match its native cursor",
		)
	}
	condition := etcdstore.Condition{
		Key:         backupvolumemanifest.CursorKey(cursor.Record.Owner.Binding),
		ModRevision: cursor.Revision,
	}
	validate := func(values []*etcdstore.KeyValue) error {
		if len(values) != 1 || values[0] == nil || values[0].Key != condition.Key ||
			values[0].ModRevision != cursor.Revision {
			return errs.New(errs.KindStateConflict, "Volume manifest cursor changed")
		}
		stored, err := backupvolumemanifest.DecodeCursor(values[0].Value)
		if err != nil || stored.Owner != cursor.Record.Owner || !stored.Complete ||
			stored.EntryCount != cursor.Record.EntryCount || stored.PointID != cursor.Record.PointID ||
			stored.Generation != cursor.Record.Generation || string(stored.ContentSHA) != string(cursor.Record.ContentSHA) ||
			string(
				stored.FullTreeSHA,
			) != string(
				cursor.Record.FullTreeSHA,
			) || string(stored.Credit) != string(cursor.Record.Credit) {
			return errs.New(errs.KindStateConflict, "Volume manifest cursor changed")
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
