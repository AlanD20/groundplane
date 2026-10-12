package handlers

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (s *Server) applyBlueprintExecutionDetails(
	ctx context.Context, parent keyvalue.Versioned[etcd.TaskRecord], request *taskShowInput, response *apiTypes.Task,
) error {
	if parent.Record.Executor != taskjournal.TaskExecutorBlueprint {
		if request.ExecutionLimit != 0 || request.ExecutionCursor != "" {
			return errs.New(
				errs.KindValidationFailed,
				"execution pagination is available for Blueprint Apply Tasks only",
			)
		}
		return nil
	}
	limit := request.ExecutionLimit
	if limit == 0 {
		limit = 5
	}
	revision := parent.ReadRevision
	if request.ExecutionCursor != "" {
		revision = 0
	}
	page, err := s.tasks.ListBlueprintTaskChildren(ctx, parent.Record, keyvalue.PageRequest{
		Limit: limit, Cursor: request.ExecutionCursor, Revision: revision,
	})
	if err != nil {
		return err
	}
	result := &apiTypes.Page[apiTypes.TaskExecution]{
		Items:      make([]apiTypes.TaskExecution, 0, len(page.Items)),
		NextCursor: page.NextCursor,
		Revision:   page.Revision,
	}
	for _, child := range page.Items {
		// Membership is cursor-pinned; execution progress must not freeze at that
		// old revision while the operator is looking beyond the first page.
		latest, err := s.tasks.GetTask(ctx, child.Record.ID)
		if err != nil {
			return err
		}
		if latest.Record.Owner != parent.Record.Owner || latest.Record.PlanID != child.Record.PlanID ||
			latest.Record.Params[taskjournal.TaskBlueprintParentParam] != parent.Record.ID ||
			!taskjournal.IsBlueprintChild(latest.Record.Params) {
			return errs.New(errs.KindInternal, "Blueprint execution does not match its captured parent")
		}
		child = latest
		events, err := s.tasks.ListTaskEvents(ctx, child.Record.ID, child.ReadRevision)
		if err != nil {
			return err
		}
		observed, err := taskResponse(child.Record, events)
		if err != nil {
			return err
		}
		recovery, err := s.tasks.GetTaskRecovery(ctx, child.Record, child.ReadRevision)
		if err != nil {
			return err
		}
		if err := applyTaskRecovery(&observed, child.Record, events, recovery); err != nil {
			return err
		}
		result.Items = append(result.Items, apiTypes.TaskExecution{
			ID: observed.ID, Type: observed.Type, Target: observed.Target, TargetName: observed.TargetName,
			ResourceKind: observed.ResourceKind, Status: observed.Status, CreatedAt: observed.CreatedAt,
			UpdatedAt: observed.UpdatedAt, StartedAt: observed.StartedAt, FinishedAt: observed.FinishedAt,
			Steps: observed.Steps, Failure: observed.Failure, Recovery: observed.Recovery,
			ReconciliationRequired: observed.ReconciliationRequired,
		})
	}
	response.Executions = result
	return nil
}
