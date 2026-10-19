package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BackupConfigWorkEvidence is supplied by the durable transfer owner, not the
// Agent report. Its revision must be compared in the checkpoint transaction.
// Only the accepted cursor is needed, not a replay of all transferred values.
type BackupConfigWorkEvidence struct {
	TaskID                   string
	WorkItemID               string
	Input                    workexecution.Input
	SourceOrdinal            uint32
	Revision                 int64
	MetadataTranscriptSHA256 []byte
	CommittedRecords         uint64
	NextOrdinal              uint32
	ValueChainSHA256         []byte
	TransferTranscriptSHA256 []byte
}

func AdvanceBackupConfigCapture(
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
	step := owned.Input.Steps[report.SourceOrdinal]
	current := owned.Sources[report.SourceOrdinal].GetCapture()
	config := step.GetCapture().GetConfig()
	if !proto.Equal(report.Attempt, owned.Attempt) || current == nil || config == nil ||
		current.CheckpointSequence == ^uint64(0) || report.CheckpointSequence != current.CheckpointSequence+1 ||
		!proto.Equal(report.PrecedingCheckpoint, current.PrecedingCheckpoint) {
		return nil, invalidBackupCheckpoint("Config capture report does not follow saved progress")
	}
	next := proto.CloneOf(current)
	if checkpoint := report.GetConfig(); checkpoint != nil {
		if current.PreparedArtifact != nil || current.GetConfigProgress().GetTransfer() != nil ||
			!validConfigWorkEvidence(evidence, attempt, report.SourceOrdinal, config.Content) {
			return nil, invalidBackupCheckpoint("Config progress has no matching committed transfer credit")
		}
		if evidence.CommittedRecords < current.Cursor.ConfigRecordSequence ||
			evidence.NextOrdinal < current.Cursor.ConfigValueOrdinal ||
			(current.GetConfigProgress() != nil && !bytes.Equal(evidence.MetadataTranscriptSHA256,
				current.GetConfigProgress().MetadataTranscriptSha256)) {
			return nil, invalidBackupCheckpoint("Config transfer progress moved backwards or changed metadata")
		}
		accepted := true
		progress := &agentpb.BackupConfigProgress{MetadataAccepted: &accepted,
			MetadataTranscriptSha256: append([]byte(nil), evidence.MetadataTranscriptSHA256...),
			NextValueOrdinal:         evidence.NextOrdinal, ValueChainSha256: append([]byte(nil), evidence.ValueChainSHA256...)}
		switch {
		case checkpoint.GetValueProgress() != nil:
			value := checkpoint.GetValueProgress()
			if value.NextOrdinal != evidence.NextOrdinal || !bytes.Equal(value.ChainSha256, evidence.ValueChainSHA256) {
				return nil, invalidBackupCheckpoint("Config value progress differs from committed transfer credit")
			}
		case checkpoint.GetTransferCompleted() != nil:
			value := checkpoint.GetTransferCompleted()
			if !proto.Equal(value.Content, config.Content) || value.RestoreGenerationId != "" ||
				value.RenderGeneration != 0 ||
				value.CommittedRecordCount != evidence.CommittedRecords ||
				evidence.NextOrdinal != config.Content.EntryCount+1 ||
				!bytes.Equal(value.ValueChainSha256, evidence.ValueChainSHA256) ||
				!backupCheckpointDigest(evidence.TransferTranscriptSHA256) ||
				!bytes.Equal(value.TransferTranscriptSha256, evidence.TransferTranscriptSHA256) {
				return nil, invalidBackupCheckpoint("Config transfer completion differs from its committed transcript")
			}
			progress.Progress = &agentpb.BackupConfigProgress_Transfer{Transfer: proto.CloneOf(value)}
		default:
			return nil, invalidBackupCheckpoint("Config capture cannot accept a restore materialization")
		}
		if !validBackupConfigResumeProgress(progress, config.Content, false) {
			return nil, invalidBackupCheckpoint("Config capture progress is invalid")
		}
		next.Cursor.ConfigRecordSequence, next.Cursor.ConfigValueOrdinal = evidence.CommittedRecords, evidence.NextOrdinal
		next.Cursor.CumulativeChainSha256 = append([]byte(nil), evidence.ValueChainSHA256...)
		next.Checkpoint = &agentpb.BackupCaptureResume_ConfigProgress{ConfigProgress: progress}
	} else if prepared := report.GetArtifactPrepared(); prepared != nil {
		transfer := current.GetConfigProgress().GetTransfer()
		if current.PreparedArtifact != nil || transfer == nil || prepared.PointId != step.GetCapture().PointId ||
			!proto.Equal(prepared.GetConfig().GetContent(), config.Content) ||
			!bytes.Equal(prepared.GetConfig().GetCaptureTranscriptSha256(), transfer.TransferTranscriptSha256) {
			return nil, invalidBackupCheckpoint("Config archive does not match its completed transfer")
		}
		next.PreparedArtifact = proto.CloneOf(prepared)
		next.Phase = agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_ARTIFACT_PREPARED
		next.Checkpoint = &agentpb.BackupCaptureResume_ArtifactPrepared{ArtifactPrepared: prepared}
	} else if err := advanceBackupCapturedObject(next, current, step, report); err != nil {
		return nil, err
	}
	next.CheckpointSequence, next.PrecedingCheckpoint = report.CheckpointSequence, nil
	return &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId,
		Operation: &agentpb.BackupStepResume_Capture{Capture: next}}, nil
}

func validConfigWorkEvidence(evidence *BackupConfigWorkEvidence, attempt workexecution.Attempt,
	ordinal uint32, content *agentpb.BackupConfigContentAuthority,
) bool {
	return evidence != nil && content != nil && evidence.Revision > 0 && evidence.TaskID == attempt.TaskID &&
		evidence.WorkItemID == attempt.WorkItemID && evidence.Input == attempt.Input && evidence.SourceOrdinal == ordinal &&
		backupCheckpointDigest(evidence.MetadataTranscriptSHA256) && evidence.CommittedRecords > 0 &&
		evidence.NextOrdinal > 0 && evidence.NextOrdinal <= content.EntryCount+1 && backupCheckpointDigest(evidence.ValueChainSHA256)
}
