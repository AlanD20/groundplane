package backup

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (owner *AgentWork) restoreSourceStaging(ctx context.Context, identity workexecution.Identity,
	entry *agentpb.BackupRecoveredStage, source backupwork.StagingSource,
) (*agentpb.BackupStagingDisposition, []keyvalue.Condition, error) {
	native, guard, err := owner.inputs.LoadStagingRestore(ctx, source)
	if err != nil {
		return nil, nil, err
	}
	step := source.Input.Steps[source.Ordinal]
	restore := step.GetRestore()
	config := restore.GetConfig() != nil
	database := restore.GetPostgres() != nil || restore.GetMysql() != nil
	if !config && !database || restore.ExpectedEvidence == nil || config && entry.DatabaseExecution {
		return nil, nil, invalidCaptureStaging()
	}
	evidence := restore.ExpectedEvidence
	full := []*agentpb.BackupRecoveredFile{
		{Role: agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT,
			SizeBytes: evidence.SourceSizeBytes, Sha256: append([]byte(nil), evidence.SourceSha256...)},
	}
	if config || restore.Encryption.GetKind() == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
		full = append(
			full,
			&agentpb.BackupRecoveredFile{Role: agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT,
				SizeBytes: evidence.StoredSizeBytes, Sha256: append([]byte(nil), evidence.StoredSha256...)},
		)
	}
	guards := append(append([]keyvalue.Condition(nil), source.Conditions...), guard)
	item := source.View.Item.Item
	settled := item.Phase == workexecution.Settled &&
		(item.Cleanup == workexecution.CleanupNone || item.Cleanup == workexecution.CleanupComplete)
	safe := native.State == backupruntime.BackupRestoreFailedSafe && !native.MutationStarted
	completed := native.State == backupruntime.BackupRestoreCompleted &&
		(config && native.ConfigProgress != nil && native.ConfigProgress.SourceCleanupCompleted ||
			database && native.DatabaseProgress != nil && native.DatabaseProgress.SourceCleanupCompleted)
	held := native.State == backupruntime.BackupRestoreRecoveryRequired && native.MutationStarted
	var decision *agentpb.BackupStagingDisposition
	if held || settled && (safe || completed) {
		partial := !held || database && native.DatabaseProgress != nil && native.DatabaseProgress.SourceCleanupCompleted
		if !recoveredRepresentationsMatch(entry.Files, full, partial) {
			return nil, nil, invalidCaptureStaging()
		}
		if held {
			digest, err := backupruntime.BackupRestoreTerminalDomainDigest(native)
			if err != nil {
				return nil, nil, err
			}
			raw, err := hex.DecodeString(digest)
			if err != nil {
				return nil, nil, err
			}
			decision = &agentpb.BackupStagingDisposition{
				RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...),
				Disposition: &agentpb.BackupStagingDisposition_RecoveryRequired{
					RecoveryRequired: &agentpb.BackupRecoveryRequired{
						NativeRestoreModRevision: guard.ModRevision, NativeRestoreSha256: raw, ExpectedFiles: proto.CloneOf(entry).Files},
				},
			}
		} else {
			decision = discardCapturedFiles(entry)
		}
	} else {
		assignment, liveGuards, err := owner.stagingAssignment(ctx, identity, source)
		if err != nil {
			return nil, nil, err
		}
		guards = append(guards, liveGuards...)
		resume := assignment.Sources[source.Ordinal].GetRestore()
		if resume == nil {
			return nil, nil, invalidCaptureStaging()
		}
		before := resume.CheckpointSequence == 0 && (!config || resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION)
		cleanup := config && resume.GetConfigProgress().GetMaterialization() != nil ||
			database && databaseFilesCleanupReady(resume, len(step.ConsumerServiceIds))
		if !recoveredRepresentationsMatch(entry.Files, full, before || database && cleanup) {
			return nil, nil, invalidCaptureStaging()
		}
		if before || cleanup {
			decision = discardCapturedFiles(entry)
		} else {
			digest, err := executionplan.BackupWorkStagingResumeSHA256(assignment, identity)
			if err != nil {
				return nil, nil, err
			}
			prepared := &agentpb.BackupResumePrepared{AssignmentResumeSha256: digest, ExpectedFiles: full,
				RemainingGrowth: agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_NO_GROWTH}
			if config {
				growth, err := backupconfigtransfer.JournalGrowthUpperBound(restore.GetConfig().GetExpectedArchive().GetContent())
				if err != nil {
					return nil, nil, err
				}
				prepared.RemainingGrowth = agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_BOUNDED
				prepared.RequiredGrowthBytes = evidence.SourceSizeBytes + growth
			}
			decision = &agentpb.BackupStagingDisposition{RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...),
				Disposition: &agentpb.BackupStagingDisposition_ResumePrepared{ResumePrepared: prepared}}
		}
	}
	if database {
		if err := attachDatabaseRecoveryGuard(source, &native, decision); err != nil {
			return nil, nil, err
		}
	}
	return decision, guards, nil
}

func recoveredRepresentationsMatch(actual, expected []*agentpb.BackupRecoveredFile, partial bool) bool {
	if !partial && len(actual) != len(expected) {
		return false
	}
	for _, file := range actual {
		matched := false
		for _, candidate := range expected {
			matched = matched || proto.Equal(file, candidate) ||
				partial && file.Role == candidate.Role && file.SizeBytes <= candidate.SizeBytes
		}
		if !matched {
			return false
		}
	}
	return true
}
