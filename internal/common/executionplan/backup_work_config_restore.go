package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// AdvanceBackupConfigRestore advances only the captured restore generation.
// The Controller must join publication/materialization ownership with the
// checkpoint; this pure transition does not authorize a desired-state rewrite.
func AdvanceBackupConfigRestore(
	assignment *agentpb.BackupWorkAssignment,
	request *agentpb.BackupWorkCheckpointRequest,
	authenticated workexecution.Identity,
	evidence *BackupConfigWorkEvidence,
) (*agentpb.BackupStepResume, error) {
	owned, attempt, err := OpenBackupWorkAssignment(assignment, authenticated)
	if err != nil {
		return nil, err
	}
	report, err := ValidateBackupWorkCheckpoint(request, owned.Input, authenticated)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(report.Attempt, owned.Attempt) {
		return nil, invalidBackupCheckpoint("Config restore report belongs to another attempt")
	}
	return advanceConfigRestoreSource(owned.Input, owned.Sources, report, evidence, attempt)
}

func advanceConfigRestoreSource(
	input *agentpb.BackupWorkInput,
	sources []*agentpb.BackupStepResume,
	report *agentpb.BackupWorkCheckpointRequest,
	evidence *BackupConfigWorkEvidence,
	transferOwner workexecution.Attempt,
) (*agentpb.BackupStepResume, error) {
	step := input.Steps[report.SourceOrdinal]
	current := sources[report.SourceOrdinal].GetRestore()
	config := step.GetRestore().GetConfig()
	if current == nil || config == nil ||
		current.CheckpointSequence == ^uint64(0) || report.CheckpointSequence != current.CheckpointSequence+1 ||
		!proto.Equal(report.PrecedingCheckpoint, current.PrecedingCheckpoint) {
		return nil, invalidBackupCheckpoint("Config restore report does not follow saved progress")
	}
	next := proto.CloneOf(current)
	switch {
	case report.GetRestoreArtifactValidated() != nil:
		value := report.GetRestoreArtifactValidated()
		if current.CheckpointSequence != 0 || !backupResumeRestoreArtifactMatches(value, step.GetRestore()) {
			return nil, invalidBackupCheckpoint("Config restore artifact differs from the captured Recovery Point")
		}
		next.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION
		next.Checkpoint = &agentpb.BackupRestoreResume_ArtifactValidated{ArtifactValidated: value}
	case report.GetConfig() != nil:
		if current.GetArtifactValidated() == nil && current.GetConfigProgress() == nil {
			return nil, invalidBackupCheckpoint("Config restore must validate its archive before transferring entries")
		}
		if err := advanceConfigRestoreTransfer(next, current, report, config, evidence, transferOwner); err != nil {
			return nil, err
		}
	case report.GetSourceCleanupCompleted() != nil:
		value := report.GetSourceCleanupCompleted()
		progress := current.GetConfigProgress()
		if progress.GetMaterialization() == nil || progress.SourceCleanupCompleted != nil ||
			value.PointId != step.GetRestore().PointId || !proto.Equal(value.Evidence, step.GetRestore().ExpectedEvidence) {
			return nil, invalidBackupCheckpoint(
				"Config cleanup must follow publication of the captured restore generation",
			)
		}
		progress = proto.CloneOf(progress)
		progress.SourceCleanupCompleted = proto.CloneOf(value)
		next.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
		next.Checkpoint = &agentpb.BackupRestoreResume_ConfigProgress{ConfigProgress: progress}
	default:
		return nil, invalidBackupCheckpoint("report is not a Config restore transition")
	}
	next.CheckpointSequence, next.PrecedingCheckpoint = report.CheckpointSequence, nil
	return &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId,
		Operation: &agentpb.BackupStepResume_Restore{Restore: next}}, nil
}

func advanceConfigRestoreTransfer(next, current *agentpb.BackupRestoreResume,
	report *agentpb.BackupWorkCheckpointRequest, config *agentpb.BackupConfigRestoreAuthority,
	evidence *BackupConfigWorkEvidence, attempt workexecution.Attempt,
) error {
	checkpoint, previous := report.GetConfig(), current.GetConfigProgress()
	content := config.ExpectedArchive.Content
	if previous.GetMaterialization() != nil {
		return invalidBackupCheckpoint("Config restore generation is already materialized")
	}
	if materialized := checkpoint.GetMaterializationVerified(); materialized != nil {
		if previous.GetTransfer() == nil || materialized.RestoreGenerationId != config.RestoreGenerationId ||
			materialized.MaterializedEntryCount != content.EntryCount {
			return invalidBackupCheckpoint("Config materialization differs from the captured restore generation")
		}
		progress := proto.CloneOf(previous)
		progress.Progress = &agentpb.BackupConfigProgress_Materialization{Materialization: proto.CloneOf(materialized)}
		next.Checkpoint = &agentpb.BackupRestoreResume_ConfigProgress{ConfigProgress: progress}
		next.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_CLEANUP
		return nil
	}
	if previous.GetTransfer() != nil || !validConfigWorkEvidence(evidence, attempt, report.SourceOrdinal, content) ||
		evidence.CommittedRecords < current.Cursor.ConfigRecordSequence || evidence.NextOrdinal < current.Cursor.ConfigValueOrdinal ||
		(previous != nil && !bytes.Equal(previous.MetadataTranscriptSha256, evidence.MetadataTranscriptSHA256)) {
		return invalidBackupCheckpoint("Config restore progress differs from committed transfer credit")
	}
	accepted := true
	progress := &agentpb.BackupConfigProgress{MetadataAccepted: &accepted,
		MetadataTranscriptSha256: append([]byte(nil), evidence.MetadataTranscriptSHA256...),
		NextValueOrdinal:         evidence.NextOrdinal, ValueChainSha256: append([]byte(nil), evidence.ValueChainSHA256...)}
	next.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION
	switch {
	case checkpoint.GetValueProgress() != nil:
		value := checkpoint.GetValueProgress()
		if value.NextOrdinal != evidence.NextOrdinal || !bytes.Equal(value.ChainSha256, evidence.ValueChainSHA256) {
			return invalidBackupCheckpoint("Config restore value progress differs from committed transfer credit")
		}
	case checkpoint.GetTransferCompleted() != nil:
		value := checkpoint.GetTransferCompleted()
		if !proto.Equal(value.Content, content) || value.RestoreGenerationId != config.RestoreGenerationId ||
			value.RenderGeneration != config.RenderGeneration || value.CommittedRecordCount != evidence.CommittedRecords ||
			evidence.NextOrdinal != content.EntryCount+1 || !bytes.Equal(value.ValueChainSha256, evidence.ValueChainSHA256) ||
			!backupCheckpointDigest(evidence.TransferTranscriptSHA256) ||
			!bytes.Equal(value.TransferTranscriptSha256, evidence.TransferTranscriptSHA256) {
			return invalidBackupCheckpoint(
				"Config restore completion differs from its captured generation or transfer transcript",
			)
		}
		progress.Progress = &agentpb.BackupConfigProgress_Transfer{Transfer: proto.CloneOf(value)}
		next.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_PUBLICATION
	default:
		return invalidBackupCheckpoint("Config restore transfer checkpoint is invalid")
	}
	if !validBackupConfigResumeProgress(progress, content, true) {
		return invalidBackupCheckpoint("Config restore progress is invalid")
	}
	next.Cursor.ConfigRecordSequence, next.Cursor.ConfigValueOrdinal = evidence.CommittedRecords, evidence.NextOrdinal
	next.Cursor.CumulativeChainSha256 = append([]byte(nil), evidence.ValueChainSHA256...)
	next.Checkpoint = &agentpb.BackupRestoreResume_ConfigProgress{ConfigProgress: progress}
	return nil
}
