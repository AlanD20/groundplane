package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
)

func ValidateVolumeRemovalCheckpointRequest(request *agentpb.VolumeRemovalCheckpointRequest) error {
	if request == nil {
		return errs.New(errs.KindValidationFailed, "Volume removal checkpoint is required")
	}
	if err := RejectUnknown(request); err != nil {
		return err
	}
	nonce, err := ulid.ParseStrict(request.RequestId)
	if err != nil || nonce.String() != request.RequestId ||
		ids.Validate(
			ids.KindTask,
			request.TaskId,
		) != nil || ids.Validate(ids.KindOperation, request.OperationId) != nil ||
		ids.Validate(
			ids.KindAssignment,
			request.AssignmentId,
		) != nil || ids.Validate(ids.KindStep, request.StepId) != nil ||
		len(request.PlanHash) != 32 || len(request.Completion) > 768*1024 ||
		request.ConsumersDetached == (len(request.Completion) != 0) {
		return errs.New(errs.KindValidationFailed, "Volume removal checkpoint identity or action is invalid")
	}
	return nil
}

func ValidateVolumeRemovalCheckpointAck(
	ack *agentpb.VolumeRemovalCheckpointAck,
	request *agentpb.VolumeRemovalCheckpointRequest,
) error {
	if err := ValidateVolumeRemovalCheckpointRequest(request); err != nil {
		return err
	}
	if ack == nil {
		return errs.New(errs.KindValidationFailed, "Volume removal checkpoint acknowledgement is required")
	}
	if err := RejectUnknown(ack); err != nil {
		return err
	}
	if ack.RequestId != request.RequestId || ack.TaskId != request.TaskId || ack.OperationId != request.OperationId ||
		ack.AssignmentId != request.AssignmentId || ack.DirectoryAbsent == (len(ack.PendingPath) != 0) ||
		len(ack.PendingPath) > 64*1024 {
		return errs.New(errs.KindStateConflict, "Volume removal checkpoint acknowledgement changed")
	}
	return nil
}
