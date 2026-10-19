package backup

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ResolveVolumeRecovery returns a decision and its atomic publication guards.
// A stopped or superseded attempt retains files; it is never translated into
// another attempt, and a deadline alone is not evidence that effects ended.
func (owner *AgentWork) ResolveVolumeRecovery(ctx context.Context, identity workexecution.Identity,
	entry *agentpb.BackupRecoveredVolumeRestore,
) (*agentpb.BackupVolumeRestoreRecoveryDisposition, []keyvalue.Condition, error) {
	source, err := owner.inputs.ReadVolumeRecoverySource(ctx, owner.queue, identity, entry)
	if err != nil {
		return nil, nil, err
	}
	native := source.Restore
	if err := matchRecoveredVolume(entry, native); err != nil {
		return nil, nil, err
	}
	digestString, err := backupruntime.BackupRestoreTerminalDomainDigest(native)
	if err != nil {
		return nil, nil, err
	}
	digest, err := hex.DecodeString(digestString)
	if err != nil {
		return nil, nil, err
	}
	result := &agentpb.BackupVolumeRestoreRecoveryDisposition{
		RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...),
	}
	view := source.View
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	settled := attempt.FinishedAt != nil && item.Phase == workexecution.Settled &&
		(item.Cleanup == workexecution.CleanupNone || item.Cleanup == workexecution.CleanupComplete)
	verifiedFiles := native.State == backupruntime.BackupRestoreVerified &&
		(!entry.JournalPresent || entry.JournalRootDeleted || entry.JournalRetiring) &&
		(!entry.ReceiverPresent || entry.ReceiverComplete || entry.ReceiverRetiring)
	if settled &&
		(native.State == backupruntime.BackupRestoreCompleted || native.State == backupruntime.BackupRestoreFailedSafe) ||
		verifiedFiles && view.ActiveAttempt && attempt.Executor == identity {
		result.Disposition = &agentpb.BackupVolumeRestoreRecoveryDisposition_Cleanup{
			Cleanup: &agentpb.BackupVolumeRestoreCleanup{
				NativeRestoreModRevision: source.Revision, NativeRestoreSha256: digest,
			},
		}
		return result, source.Conditions, nil
	}
	now := time.Now().UTC()
	canResume := native.State != backupruntime.BackupRestoreRecoveryRequired &&
		native.State != backupruntime.BackupRestoreFailedSafe && native.State != backupruntime.BackupRestoreCompleted &&
		view.ActiveAttempt && view.CurrentExecutor && attempt.Executor == identity &&
		attempt.StartedAt != nil && attempt.FinishedAt == nil && task.FinishedAt == nil &&
		task.Stop == nil && !item.ForwardRevoked &&
		now.Before(attempt.Deadline) && now.Before(item.Deadline) && now.Before(task.Deadline) &&
		!entry.JournalPartial && !entry.JournalRetiring && !entry.ReceiverRetiring
	if !canResume {
		if entry.JournalRetiring || entry.ReceiverRetiring {
			return nil, nil, invalidRecoveredVolume()
		}
		result.Disposition = &agentpb.BackupVolumeRestoreRecoveryDisposition_Hold{
			Hold: &agentpb.BackupVolumeRestoreHold{
				NativeRestoreModRevision: source.Revision, NativeRestoreSha256: digest,
			},
		}
		return result, source.Conditions, nil
	}
	assignment, guarded, err := owner.inputs.LoadAssignment(ctx, view)
	if err != nil {
		return nil, nil, err
	}
	resumeDigest, err := executionplan.BackupWorkStagingResumeSHA256(assignment, identity)
	if err != nil {
		return nil, nil, err
	}
	result.Disposition = &agentpb.BackupVolumeRestoreRecoveryDisposition_Resume{
		Resume: &agentpb.BackupVolumeRestoreResume{
			AssignmentResumeSha256: resumeDigest, NativeRestoreModRevision: source.Revision, NativeRestoreSha256: digest,
		},
	}
	return result, append(source.Conditions, guarded.Conditions...), nil
}

func matchRecoveredVolume(entry *agentpb.BackupRecoveredVolumeRestore, native backupruntime.BackupRestoreRecord) error {
	if native.Point.SourceKind != backupruntime.BackupRuntimeSourceVolume ||
		(entry.JournalDiscarding || entry.ReceiverDiscarding) && native.State != backupruntime.BackupRestoreFailedSafe {
		return invalidRecoveredVolume()
	}
	archive := native.Point.VolumeArchive
	if entry.ReceiverPresent &&
		(hex.EncodeToString(entry.ReceiverContentManifestSha256) != archive.ContentManifestSHA256 ||
			hex.EncodeToString(entry.ReceiverFullTreeSha256) != archive.FullTreeSHA256 ||
			hex.EncodeToString(entry.ReceiverSourceSha256) != native.Point.Evidence.SourceSHA256 ||
			entry.ReceiverEntryCount != archive.EntryCount) {
		return invalidRecoveredVolume()
	}
	if entry.JournalPresent && (hex.EncodeToString(entry.NewManifestSha256) != archive.ContentManifestSHA256 ||
		hex.EncodeToString(entry.NewFullTreeSha256) != archive.FullTreeSHA256 ||
		entry.NewEntryCount != archive.EntryCount || native.VolumeProgress == nil ||
		hex.EncodeToString(entry.OldManifestSha256) != native.VolumeProgress.OldContentManifestSHA256 ||
		hex.EncodeToString(entry.OldFullTreeSha256) != native.VolumeProgress.OldFullTreeSHA256 ||
		entry.OldEntryCount != native.VolumeProgress.OldEntryCount) {
		return invalidRecoveredVolume()
	}
	return nil
}

func invalidRecoveredVolume() error {
	return errs.New(errs.KindStateConflict, "Retained Volume recovery files do not match the saved Restore state")
}
