package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Reports struct {
	queue *workqueue.Repository
	owner *AgentWork
}

func NewReports(queue *workqueue.Repository, owner *AgentWork) (*Reports, error) {
	if queue == nil || owner == nil {
		return nil, errs.New(errs.KindInternal, "Environment removal reporting requires storage and its execution owner")
	}
	return &Reports{queue: queue, owner: owner}, nil
}

func (service *Reports) CheckpointEnvironmentRemoval(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.EnvironmentRemovalCheckpointRequest,
) (*agentpb.EnvironmentRemovalCheckpointAck, error) {
	if ctx == nil || request == nil || len(request.Report) == 0 || len(request.Report) > hierarchyexecution.MaximumEnvironmentReportBytes {
		return nil, errs.New(errs.KindValidationFailed, "Environment removal report is missing or oversized")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	view, err := service.queue.ReadResume(ctx, claimed.TaskID, claimed.WorkItemID, claimed.ID, authorization.Identity())
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(claimed, view.Attempt.Attempt) || view.Attempt.Attempt.StartedAt == nil ||
		view.Work.Task.Task.CollectionStartedAt != nil || !view.Work.Task.Task.PublicationComplete {
		return nil, errs.New(errs.KindStateConflict, "Environment removal report has no retained started execution")
	}
	message, err := service.owner.capturedEnvironmentAssignment(ctx, view)
	if err != nil {
		return nil, err
	}
	input, err := hierarchyexecution.DecodeEnvironmentAssignment(message, authorization.Identity())
	if err != nil {
		return nil, err
	}
	input.Attempt = view.Attempt.Attempt
	report, err := hierarchyexecution.DecodeEnvironmentCheckpointRequest(input, request)
	if err != nil {
		return nil, err
	}
	if report.Progress.UpdatedAt.After(time.Now().UTC()) {
		return nil, errs.New(errs.KindValidationFailed, "Environment removal report time is in the future")
	}
	change, err := service.owner.records.PrepareHierarchyEnvironmentCheckpoint(ctx, view, input, report)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Environment removal ownership changed; resend the saved report")
	}
	return hierarchyexecution.EncodeEnvironmentCheckpointAck(input, report)
}
