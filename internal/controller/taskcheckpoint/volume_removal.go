package taskcheckpoint

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type VolumeRemovalCheckpointService struct {
	repository *volumeremoval.EnvironmentVolumeRemovalRuntimeRepository
	now        func() time.Time
}

func NewVolumeRemovalCheckpointService(
	repository *volumeremoval.EnvironmentVolumeRemovalRuntimeRepository,
) (*VolumeRemovalCheckpointService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "Volume removal checkpoint repository is required")
	}
	return &VolumeRemovalCheckpointService{repository: repository, now: time.Now}, nil
}

func (service *VolumeRemovalCheckpointService) CheckpointVolumeRemoval(
	ctx context.Context, agentID string, generation uint64, request *agentpb.VolumeRemovalCheckpointRequest,
) (*agentpb.VolumeRemovalCheckpointAck, error) {
	if ctx == nil || service == nil || service.repository == nil {
		return nil, errs.New(errs.KindInternal, "Volume removal checkpoint service is not configured")
	}
	if err := executionplan.ValidateVolumeRemovalCheckpointRequest(request); err != nil {
		return nil, err
	}
	assignment := volumeremoval.EnvironmentVolumeRemovalAssignment{
		OperationID: request.OperationId, TaskID: request.TaskId, AssignmentID: request.AssignmentId,
		AgentID: agentID, AgentGeneration: generation,
	}
	state, err := service.repository.ResumeAssigned(
		ctx,
		assignment,
		request.StepId,
		hex.EncodeToString(request.PlanHash),
	)
	if err != nil {
		return nil, err
	}
	ack := &agentpb.VolumeRemovalCheckpointAck{RequestId: request.RequestId, TaskId: request.TaskId,
		OperationId: request.OperationId, AssignmentId: request.AssignmentId}
	if request.ConsumersDetached {
		if state.Runtime.Record.Checkpoint == removal.DesiredPublished {
			if _, err := service.repository.MarkConsumersDetached(ctx, assignment, service.now().UTC()); err != nil {
				return nil, err
			}
		}
		ack.DirectoryAbsent = state.Runtime.Record.Checkpoint == removal.DirectoryAbsent &&
			state.Progress.Record.DirectoryAbsent
	} else {
		completion, err := removal.DecodeCompletion(request.Completion)
		if err != nil {
			return nil, err
		}
		if completion.OperationID != request.OperationId || completion.CompletedAt.Before(state.Runtime.Record.CreatedAt) ||
			completion.CompletedAt.After(service.now().UTC()) {
			return nil, errs.New(errs.KindStateConflict, "Volume helper completion identity or time changed")
		}
		progress, _, err := service.repository.CompletePathCall(ctx, volumeremoval.EnvironmentVolumeRemovalPathResult{
			Assignment: assignment, RequestOrdinal: completion.RequestOrdinal, RequestSHA256: completion.RequestSHA256,
			ResponseSHA256: completion.ResponseSHA256, ResponseBytes: completion.ResponseBytes, MutationCount: completion.MutationCount,
			NextComponentStack: completion.NextComponentStack, NextCursor: completion.NextCursor, DirectoryAbsent: completion.DirectoryAbsent,
			CompletedAt: completion.CompletedAt,
		})
		if err != nil {
			return nil, err
		}
		ack.DirectoryAbsent = progress.Record.DirectoryAbsent
	}
	if !ack.DirectoryAbsent {
		pending, _, err := service.repository.BeginPathCall(ctx, assignment, service.now().UTC())
		if err != nil {
			return nil, err
		}
		ack.PendingPath, err = removal.EncodePendingPath(pending.Record)
		if err != nil {
			return nil, err
		}
	}
	return ack, nil
}
