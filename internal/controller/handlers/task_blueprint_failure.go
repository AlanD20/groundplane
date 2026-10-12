package handlers

import (
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A private child is execution of the visible Apply, not another operator action.
// Project its recorded failure without exposing a child route or changing history.
func (s *Server) applyBlueprintFailureDetails(
	ctx context.Context, parent etcd.TaskRecord, response *apiTypes.Task,
) error {
	failure := parent.BlueprintFailure
	if failure == nil || failure.Phase != "child_execution" {
		return nil
	}
	child, err := s.tasks.GetTask(ctx, failure.ChildTaskID)
	if err != nil {
		if kind, _ := errs.KindOf(err); kind == errs.KindTaskNotFound {
			response.Failure = &apiTypes.TaskFailure{
				Status:  string(response.Status),
				Summary: "Blueprint Apply failed while executing a resource change. Its execution details are unavailable; a new Apply is required.",
			}
			return nil
		}
		return err
	}
	if !taskjournal.IsBlueprintChild(child.Record.Params) ||
		child.Record.Params[taskjournal.TaskBlueprintParentParam] != parent.ID ||
		child.Record.PlanID != failure.ChildPlanID || child.Record.Owner != parent.Owner {
		return errs.New(errs.KindInternal, "Blueprint failed child does not match its recorded owner")
	}
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
	response.Recovery = observed.Recovery
	response.ReconciliationRequired = response.ReconciliationRequired || observed.ReconciliationRequired
	response.ResultSummary = observed.ResultSummary
	if observed.Failure != nil {
		response.Failure = observed.Failure
		response.Failure.Status = string(response.Status)
		resource := child.Record.TargetName
		if resource == "" {
			resource = child.Record.Target
		}
		response.Failure.Summary = fmt.Sprintf(
			"Blueprint Apply failed for %s: %s A new Apply is required.",
			resource,
			observed.Failure.Summary,
		)
	}
	return nil
}
