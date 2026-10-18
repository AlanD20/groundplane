package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateBackupWorkCheckpoint binds a typed report to its captured source and
// authenticated attempt. The durable owner additionally checks its next sequence
// and permitted source transition before acknowledging any external effect.
func ValidateBackupWorkCheckpoint(request *agentpb.BackupWorkCheckpointRequest,
	input *agentpb.BackupWorkInput, authenticated workexecution.Identity,
) (*agentpb.BackupWorkCheckpointRequest, error) {
	if request == nil || proto.Size(request) > MaximumPlanBytes || RejectUnknown(request) != nil ||
		request.CheckpointSequence == 0 {
		return nil, invalidBackupCheckpoint("work report is invalid")
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, authenticated)
	if err != nil {
		return nil, err
	}
	ownedInput, err := OpenBackupWorkInput(attempt, input)
	if err != nil {
		return nil, err
	}
	if request.GetDatabaseConsumerRecovery() != nil {
		return nil, invalidBackupCheckpoint("consumer-only recovery is not a forward execution action")
	}
	return validateBackupSourceCheckpoint(request, ownedInput)
}

func validateBackupSourceCheckpoint(request *agentpb.BackupWorkCheckpointRequest, ownedInput *agentpb.BackupWorkInput) (*agentpb.BackupWorkCheckpointRequest, error) {
	if request == nil || proto.Size(request) > MaximumPlanBytes || RejectUnknown(request) != nil || request.CheckpointSequence == 0 {
		return nil, invalidBackupCheckpoint("work report is invalid")
	}
	if uint64(request.SourceOrdinal) >= uint64(len(ownedInput.Steps)) {
		return nil, invalidBackupCheckpoint("source ordinal is outside captured input")
	}
	step := ownedInput.Steps[request.SourceOrdinal]
	if request.CheckpointSequence == 1 {
		if request.PrecedingCheckpoint != nil {
			return nil, invalidBackupCheckpoint("first work checkpoint cannot name a predecessor")
		}
	} else if !backupCheckpointFence(request.PrecedingCheckpoint, step.StepDigest) {
		return nil, invalidBackupCheckpoint("work checkpoint predecessor belongs to another source")
	}
	if err := validateBackupWorkCheckpointPayload(request); err != nil {
		return nil, err
	}
	return proto.CloneOf(request), nil
}

func BackupWorkCheckpointDigest(request *agentpb.BackupWorkCheckpointRequest) ([]byte, error) {
	if request == nil || proto.Size(request) > MaximumPlanBytes || RejectUnknown(request) != nil ||
		validateBackupWorkCheckpointPayload(request) != nil {
		return nil, invalidBackupCheckpoint("work report cannot be hashed")
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(raw)
	hash := sha256.New()
	_, _ = hash.Write([]byte("groundplane.backup.work-checkpoint.v1\x00"))
	_, _ = hash.Write(raw)
	return hash.Sum(nil), nil
}

func ValidateBackupWorkCheckpointAck(ack *agentpb.BackupWorkCheckpointAck,
	request *agentpb.BackupWorkCheckpointRequest, input *agentpb.BackupWorkInput, authenticated workexecution.Identity,
) (*agentpb.BackupWorkCheckpointAck, error) {
	validated, err := ValidateBackupWorkCheckpoint(request, input, authenticated)
	if err != nil {
		return nil, err
	}
	return validateBackupSourceCheckpointAck(ack, validated, input)
}

func validateBackupSourceCheckpointAck(ack *agentpb.BackupWorkCheckpointAck, validated *agentpb.BackupWorkCheckpointRequest, input *agentpb.BackupWorkInput) (*agentpb.BackupWorkCheckpointAck, error) {
	digest, err := BackupWorkCheckpointDigest(validated)
	if err != nil {
		return nil, err
	}
	if ack == nil || RejectUnknown(ack) != nil || !proto.Equal(ack.Attempt, validated.Attempt) ||
		ack.SourceOrdinal != validated.SourceOrdinal || ack.CheckpointSequence != validated.CheckpointSequence ||
		!bytes.Equal(ack.RequestSha256, digest) ||
		!backupCheckpointFence(ack.Committed, input.Steps[validated.SourceOrdinal].StepDigest) ||
		ack.Committed.DedupeKeyModRevision <= validated.GetPrecedingCheckpoint().GetDedupeKeyModRevision() {
		return nil, invalidBackupCheckpoint("work acknowledgement does not prove this report committed")
	}
	if err := ValidateBackupSourceResumes([]*agentpb.BackupStepResume{ack.Source},
		[]*agentpb.BackupStepAuthority{input.Steps[ack.SourceOrdinal]}); err != nil {
		return nil, err
	}
	var sequence uint64
	var fence *agentpb.CheckpointFence
	switch {
	case ack.Source.GetCapture() != nil:
		sequence, fence = ack.Source.GetCapture().CheckpointSequence, ack.Source.GetCapture().PrecedingCheckpoint
	case ack.Source.GetRestore() != nil:
		sequence, fence = ack.Source.GetRestore().CheckpointSequence, ack.Source.GetRestore().PrecedingCheckpoint
	case ack.Source.GetPrune() != nil:
		sequence, fence = ack.Source.GetPrune().CheckpointSequence, ack.Source.GetPrune().PrecedingCheckpoint
	}
	if sequence != ack.CheckpointSequence || !proto.Equal(fence, ack.Committed) {
		return nil, invalidBackupCheckpoint("work acknowledgement source differs from its receipt")
	}
	if err := validateDatabaseWorkProgress(ack.Source.GetRestore(), input.Steps[ack.SourceOrdinal], input.Services); err != nil {
		return nil, err
	}
	return proto.CloneOf(ack), nil
}
