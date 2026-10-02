package agentchannel

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Restore staging is never inferred from a Capture receipt. Before validation
// it is a disposable exact-object download; after validation only the selected
// full representations can resume. A materialization receipt authorizes their
// cleanup, not another file application.
func (s *Server) resolveConfigRestoreDisposition(ctx context.Context, store backupStagingStore,
	entry *agentpb.BackupRecoveredStage, source etcd.BackupStagingSource,
) (*agentpb.BackupStagingDisposition, error) {
	if taskjournal.IsTerminalTaskStatus(source.Task.Record.Status) {
		return terminalConfigRestoreDisposition(entry, source)
	}
	if source.Task.Record.Status != taskjournal.TaskStatusRunning || source.Assignment == nil {
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
	var resume *agentpb.BackupRestoreResume
	for _, retained := range message.BackupResume.Steps {
		if retained.StepId == source.Step.StepId && retained.ExecutionId == source.Step.ExecutionId {
			resume = retained.GetRestore()
			break
		}
	}
	if resume == nil {
		return nil, unresolvedBackupStage()
	}
	expected := source.Step.GetRestore().ExpectedEvidence
	full := []*agentpb.BackupRecoveredFile{
		{Role: agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT,
			SizeBytes: expected.SourceSizeBytes, Sha256: expected.SourceSha256},
		{Role: agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT,
			SizeBytes: expected.StoredSizeBytes, Sha256: expected.StoredSha256},
	}
	beforeValidation := resume.CheckpointSequence == 0 &&
		resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION
	cleanup := resume.GetConfigProgress().GetMaterialization() != nil
	for _, file := range entry.Files {
		matched := false
		for _, representation := range full {
			if beforeValidation {
				matched = matched || (file.Role == representation.Role && file.SizeBytes <= representation.SizeBytes)
			} else {
				matched = matched || proto.Equal(file, representation)
			}
		}
		if !matched {
			return nil, unresolvedBackupStage()
		}
	}
	result := &agentpb.BackupStagingDisposition{RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...)}
	if beforeValidation || cleanup {
		result.Disposition = &agentpb.BackupStagingDisposition_DiscardRecovered{
			DiscardRecovered: &agentpb.BackupDiscardRecovered{},
		}
		return result, nil
	}
	if len(entry.Files) != len(full) {
		return nil, unresolvedBackupStage()
	}
	resumeSHA, err := executionplan.BackupStagingAssignmentResumeSHA256(message.BackupAuthority, message.BackupResume)
	if err != nil {
		return nil, err
	}
	content := source.Step.GetRestore().GetConfig().GetExpectedArchive().GetContent()
	journalGrowth, err := backupconfigtransfer.JournalGrowthUpperBound(content)
	if err != nil {
		return nil, err
	}
	result.Disposition = &agentpb.BackupStagingDisposition_ResumePrepared{ResumePrepared: &agentpb.BackupResumePrepared{
		AssignmentResumeSha256: resumeSHA, RemainingGrowth: agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_BOUNDED,
		RequiredGrowthBytes: expected.SourceSizeBytes + journalGrowth, ExpectedFiles: full,
	}}
	return result, nil
}

func terminalConfigRestoreDisposition(entry *agentpb.BackupRecoveredStage,
	source etcd.BackupStagingSource,
) (*agentpb.BackupStagingDisposition, error) {
	// ReadBackupStagingSource has verified the atomic terminal receipt and
	// exact native outcome. Task state alone is never cleanup authority.
	assignment := source.Task.Record.TerminalAssignment
	native := source.TerminalRestore
	if source.Assignment != nil || native == nil || assignment == nil ||
		assignment.AgentID != source.Index.Record.AgentID ||
		assignment.AgentGeneration != source.Index.Record.AgentGeneration {
		return nil, unresolvedBackupStage()
	}
	safe := native.State == backupruntime.BackupRestoreFailedSafe && !native.MutationStarted
	completed := native.State == backupruntime.BackupRestoreCompleted && native.ConfigProgress != nil &&
		native.ConfigProgress.SourceCleanupCompleted
	recoveryRequired := native.State == backupruntime.BackupRestoreRecoveryRequired && native.MutationStarted
	if !safe && !completed && !recoveryRequired {
		return nil, unresolvedBackupStage()
	}
	expected := source.Step.GetRestore().ExpectedEvidence
	for _, file := range entry.Files {
		var maximum uint64
		var digest []byte
		switch file.Role {
		case agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT:
			maximum = expected.SourceSizeBytes
			digest = expected.SourceSha256
		case agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT:
			maximum = expected.StoredSizeBytes
			digest = expected.StoredSha256
		default:
			return nil, unresolvedBackupStage()
		}
		if file.SizeBytes > maximum ||
			recoveryRequired && (file.SizeBytes != maximum || !bytes.Equal(file.Sha256, digest)) {
			return nil, unresolvedBackupStage()
		}
	}
	if recoveryRequired {
		digest, err := backupruntime.BackupRestoreTerminalDomainDigest(*native)
		if err != nil {
			return nil, err
		}
		raw, err := hex.DecodeString(digest)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		return &agentpb.BackupStagingDisposition{
			RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...),
			Disposition: &agentpb.BackupStagingDisposition_RecoveryRequired{
				RecoveryRequired: &agentpb.BackupRecoveryRequired{
					NativeRestoreModRevision: source.Task.Revision,
					NativeRestoreSha256:      raw,
					ExpectedFiles:            proto.CloneOf(entry).Files,
				},
			},
		}, nil
	}
	return &agentpb.BackupStagingDisposition{
		RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...),
		Disposition: &agentpb.BackupStagingDisposition_DiscardRecovered{
			DiscardRecovered: &agentpb.BackupDiscardRecovered{},
		},
	}, nil
}
