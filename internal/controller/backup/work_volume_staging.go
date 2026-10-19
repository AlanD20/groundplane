package backup

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (owner *AgentWork) volumeRestoreStaging(ctx context.Context, identity workexecution.Identity,
	entry *agentpb.BackupRecoveredStage, source backupwork.StagingSource,
) (*agentpb.BackupStagingDisposition, []keyvalue.Condition, error) {
	native, guard, err := owner.inputs.LoadStagingRestore(ctx, source)
	if err != nil {
		return nil, nil, err
	}
	step := source.Input.Steps[source.Ordinal]
	restore := step.GetRestore()
	if restore.GetVolume() == nil || native.Point.SourceKind != backupruntime.BackupRuntimeSourceVolume ||
		entry.DatabaseExecution {
		return nil, nil, invalidCaptureStaging()
	}
	evidence := restore.ExpectedEvidence
	if evidence == nil {
		return nil, nil, invalidCaptureStaging()
	}
	full := []*agentpb.BackupRecoveredFile{
		{Role: agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT,
			SizeBytes: evidence.SourceSizeBytes, Sha256: append([]byte(nil), evidence.SourceSha256...)},
	}
	if restore.Encryption.GetKind() == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
		full = append(
			full,
			&agentpb.BackupRecoveredFile{Role: agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT,
				SizeBytes: evidence.StoredSizeBytes, Sha256: append([]byte(nil), evidence.StoredSha256...)},
		)
	}
	guards := append(append([]keyvalue.Condition(nil), source.Conditions...), guard)
	result := &agentpb.BackupStagingDisposition{RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...)}
	item := source.View.Item.Item
	settled := item.Phase == workexecution.Settled &&
		(item.Cleanup == workexecution.CleanupNone || item.Cleanup == workexecution.CleanupComplete)
	if settled &&
		(native.State == backupruntime.BackupRestoreCompleted || native.State == backupruntime.BackupRestoreFailedSafe) {
		result.Disposition = &agentpb.BackupStagingDisposition_DiscardRecovered{
			DiscardRecovered: &agentpb.BackupDiscardRecovered{},
		}
		return result, guards, nil
	}
	resume := source.Sources[source.Ordinal].GetRestore()
	if resume == nil {
		return nil, nil, invalidCaptureStaging()
	}
	beforeValidation := resume.CheckpointSequence == 0
	for _, file := range entry.Files {
		matched := false
		for _, expected := range full {
			if beforeValidation {
				matched = matched || file.Role == expected.Role && file.SizeBytes <= expected.SizeBytes
			} else {
				matched = matched || proto.Equal(file, expected)
			}
		}
		if !matched {
			return nil, nil, invalidCaptureStaging()
		}
	}
	if native.State == backupruntime.BackupRestoreRecoveryRequired {
		if !native.MutationStarted || !sameRecoveredFiles(entry.Files, full) {
			return nil, nil, invalidCaptureStaging()
		}
		encoded, err := backupruntime.BackupRestoreTerminalDomainDigest(native)
		if err != nil {
			return nil, nil, err
		}
		digest, err := hex.DecodeString(encoded)
		if err != nil {
			return nil, nil, err
		}
		result.Disposition = &agentpb.BackupStagingDisposition_RecoveryRequired{
			RecoveryRequired: &agentpb.BackupRecoveryRequired{
				NativeRestoreModRevision: guard.ModRevision, NativeRestoreSha256: digest, ExpectedFiles: proto.CloneOf(entry).Files},
		}
		return result, guards, nil
	}
	assignment, liveGuards, err := owner.stagingAssignment(ctx, identity, source)
	if err != nil {
		return nil, nil, err
	}
	guards = append(guards, liveGuards...)
	if beforeValidation || native.State == backupruntime.BackupRestoreVerified {
		result.Disposition = &agentpb.BackupStagingDisposition_DiscardRecovered{
			DiscardRecovered: &agentpb.BackupDiscardRecovered{},
		}
		return result, guards, nil
	}
	if !sameRecoveredFiles(entry.Files, full) {
		return nil, nil, invalidCaptureStaging()
	}
	digest, err := executionplan.BackupWorkStagingResumeSHA256(assignment, identity)
	if err != nil {
		return nil, nil, err
	}
	result.Disposition = &agentpb.BackupStagingDisposition_ResumePrepared{ResumePrepared: &agentpb.BackupResumePrepared{
		AssignmentResumeSha256: digest, ExpectedFiles: full,
		RemainingGrowth:     agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_BOUNDED,
		RequiredGrowthBytes: 2 * backupvolume.RestoreHeadroom}}
	return result, guards, nil
}

func sameRecoveredFiles(actual, expected []*agentpb.BackupRecoveredFile) bool {
	if len(actual) != len(expected) {
		return false
	}
	for index := range actual {
		if !proto.Equal(actual[index], expected[index]) {
			return false
		}
	}
	return true
}
