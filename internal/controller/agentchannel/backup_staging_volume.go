package agentchannel

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (s *Server) resolveBackupVolumeRestoreDisposition(ctx context.Context,
	store backupStagingStore, agentID string, generation uint64,
	entry *agentpb.BackupRecoveredVolumeRestore, source etcd.BackupStagingSource,
) (*agentpb.BackupVolumeRestoreRecoveryDisposition, error) {
	if entry == nil || source.Step == nil || source.Step.GetRestore().GetVolume() == nil ||
		source.Task.Record.Type != taskjournal.TaskRestore || source.VolumeRestore == nil ||
		source.Task.Record.ID != source.Index.Record.TaskID ||
		source.Index.Record.StepID != source.Step.StepId ||
		source.Index.Record.PointID != source.Step.GetRestore().PointId ||
		source.Index.Record.AgentID != agentID || source.Index.Record.AgentGeneration != generation ||
		source.Step.ExecutionId != entry.RestoreGenerationId ||
		!bytes.Equal(source.Step.StepDigest, entry.StepSha256) ||
		source.VolumeRestore.Record.RestoreGenerationID != entry.RestoreGenerationId ||
		source.VolumeRestore.Record.Point.ID != source.Index.Record.PointID {
		return nil, unresolvedBackupStage()
	}
	native := source.VolumeRestore.Record
	if (entry.JournalDiscarding || entry.ReceiverDiscarding) &&
		native.State != backupruntime.BackupRestoreFailedSafe {
		return nil, unresolvedBackupStage()
	}
	archive := native.Point.VolumeArchive
	if entry.ReceiverPresent &&
		(hex.EncodeToString(entry.ReceiverContentManifestSha256) != archive.ContentManifestSHA256 ||
			hex.EncodeToString(entry.ReceiverFullTreeSha256) != archive.FullTreeSHA256 ||
			hex.EncodeToString(entry.ReceiverSourceSha256) != native.Point.Evidence.SourceSHA256 ||
			entry.ReceiverEntryCount != archive.EntryCount) {
		return nil, unresolvedBackupStage()
	}
	if entry.JournalPresent && (hex.EncodeToString(entry.NewManifestSha256) != archive.ContentManifestSHA256 ||
		hex.EncodeToString(entry.NewFullTreeSha256) != archive.FullTreeSHA256 ||
		entry.NewEntryCount != archive.EntryCount || native.VolumeProgress == nil ||
		hex.EncodeToString(entry.OldManifestSha256) != native.VolumeProgress.OldContentManifestSHA256 ||
		hex.EncodeToString(entry.OldFullTreeSha256) != native.VolumeProgress.OldFullTreeSHA256 ||
		entry.OldEntryCount != native.VolumeProgress.OldEntryCount) {
		return nil, unresolvedBackupStage()
	}
	result := &agentpb.BackupVolumeRestoreRecoveryDisposition{
		RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...)}
	digestString, err := backupruntime.BackupRestoreTerminalDomainDigest(native)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(digestString)
	if err != nil {
		return nil, err
	}
	terminal := taskjournal.IsTerminalTaskStatus(source.Task.Record.Status)
	if terminal {
		if source.Task.Record.TerminalAssignment == nil ||
			source.Task.Record.TerminalAssignment.AgentID != agentID ||
			source.Task.Record.TerminalAssignment.AgentGeneration != source.Index.Record.AgentGeneration ||
			source.Task.Record.Result == nil {
			return nil, unresolvedBackupStage()
		}
		if native.State == backupruntime.BackupRestoreRecoveryRequired ||
			source.Task.Record.Result.ReconciliationRequired {
			if entry.JournalRetiring || entry.ReceiverRetiring {
				return nil, unresolvedBackupStage()
			}
			result.Disposition = &agentpb.BackupVolumeRestoreRecoveryDisposition_Hold{
				Hold: &agentpb.BackupVolumeRestoreHold{
					NativeRestoreModRevision: source.VolumeRestore.Revision, NativeRestoreSha256: digest}}
			return result, nil
		}
		if native.State != backupruntime.BackupRestoreCompleted &&
			native.State != backupruntime.BackupRestoreFailedSafe {
			return nil, unresolvedBackupStage()
		}
		result.Disposition = &agentpb.BackupVolumeRestoreRecoveryDisposition_Cleanup{
			Cleanup: &agentpb.BackupVolumeRestoreCleanup{
				NativeRestoreModRevision: source.VolumeRestore.Revision, NativeRestoreSha256: digest}}
		return result, nil
	}
	if source.Assignment == nil || source.Assignment.Record.AssignmentID != entry.AssignmentId ||
		source.Assignment.Record.AgentID != agentID || source.Assignment.Record.AgentGeneration != generation ||
		entry.JournalPartial || entry.JournalRetiring || entry.ReceiverRetiring {
		return nil, unresolvedBackupStage()
	}
	claim, err := store.GetTaskAssignment(ctx, source.Task.Record.ID)
	if err != nil {
		return nil, err
	}
	if claim.Task.Revision != source.Task.Revision || claim.Assignment.Revision != source.Assignment.Revision {
		return nil, unresolvedBackupStage()
	}
	message, err := s.taskAssignmentMessage(ctx, claim, true)
	if err != nil {
		return nil, err
	}
	if native.State == backupruntime.BackupRestoreVerified &&
		(!entry.JournalPresent || entry.JournalRootDeleted || entry.JournalRetiring) &&
		(!entry.ReceiverPresent || entry.ReceiverComplete || entry.ReceiverRetiring) {
		result.Disposition = &agentpb.BackupVolumeRestoreRecoveryDisposition_Cleanup{
			Cleanup: &agentpb.BackupVolumeRestoreCleanup{
				NativeRestoreModRevision: source.VolumeRestore.Revision, NativeRestoreSha256: digest}}
		return result, nil
	}
	resumeSHA, err := executionplan.BackupStagingAssignmentResumeSHA256(message.BackupAuthority, message.BackupResume)
	if err != nil {
		return nil, err
	}
	result.Disposition = &agentpb.BackupVolumeRestoreRecoveryDisposition_Resume{
		Resume: &agentpb.BackupVolumeRestoreResume{AssignmentResumeSha256: resumeSHA,
			NativeRestoreModRevision: source.VolumeRestore.Revision, NativeRestoreSha256: digest}}
	return result, nil
}
