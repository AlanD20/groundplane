package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *AgentWork) RemovalInput(ctx context.Context, view workqueue.ResumeView) (attachexecution.RemovalInput, error) {
	return owner.capturedAttachInput(ctx, view)
}

func (owner *AgentWork) RecoveryInput(ctx context.Context, view workqueue.ResumeView) (attachexecution.RecoveryInput, error) {
	source, err := owner.records.LoadHierarchyAttachRecovery(ctx, view.Work)
	if err != nil {
		return attachexecution.RecoveryInput{}, err
	}
	input := attachexecution.RecoveryInput{Attempt: view.Attempt.Attempt, Source: source.Source}
	return input, input.Validate()
}

func (owner *AgentWork) PrepareRemovalCheckpoint(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, report attachexecution.RemovalReport,
) (workqueue.Change, error) {
	return owner.records.PrepareHierarchyAttachCheckpoint(ctx, view, input, report)
}

func (owner *AgentWork) PrepareRemovalCompletion(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, at time.Time,
) (workqueue.Change, error) {
	return owner.records.PrepareHierarchyAttachCompletion(ctx, view, input, at)
}

func (owner *AgentWork) PrepareRemovalFailure(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, reason attachexecution.FailureReason, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	return owner.records.PrepareHierarchyAttachFailure(ctx, view, input, reason, at)
}

func (owner *AgentWork) LoadRemovalFailure(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	return owner.records.LoadHierarchyAttachFailure(ctx, view)
}

func (owner *AgentWork) PrepareRemovalInspection(ctx context.Context, view workqueue.ResumeView,
	report attachexecution.RemovalInspection, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	return owner.records.PrepareHierarchyAttachInspection(ctx, view, report, at)
}

func (owner *AgentWork) LoadRemovalInspection(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	return owner.records.LoadHierarchyAttachInspection(ctx, view)
}
