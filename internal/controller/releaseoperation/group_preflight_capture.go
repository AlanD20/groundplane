package releaseoperation

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) captureGroupPreflight(ctx context.Context, task workexecution.Task, ordinal uint32,
	member *etcd.GroupReleaseMemberCapture, capture *etcd.GroupReleaseCapture,
) error {
	operation := member.Preparation.Intent.OperationKind
	phases, err := releaserender.OrderedReleaseHooks(operation, member.Rollout.Render.ServiceID, member.Rollout.Render.Hooks)
	if err != nil {
		return err
	}
	defer phases.Clear()
	if len(phases.Before) == 0 {
		return nil
	}
	if len(capture.Preflight)+len(phases.Before) > 16 {
		return errs.New(errs.KindReleasePlanTooLarge, "Group selects more than sixteen required preflight hooks")
	}
	for _, hook := range phases.Before {
		index := slices.IndexFunc(member.Preparation.Hooks, func(source etcd.DirectReleaseHookSources) bool {
			return source.ExecutionID == hook.ScriptExecutionID
		})
		if index < 0 {
			return errs.New(errs.KindStateConflict, "Group preflight has no captured hook sources")
		}
		sources := member.Preparation.Hooks[index].Sources
		prepared, err := service.preparation.Prepare(ctx, sources)
		if err != nil {
			return err
		}
		workID, operationID, planID := ids.New(ids.KindWorkItem), ids.New(ids.KindOperation), ids.New(ids.KindPlan)
		runner, err := taskplanning.PrepareReleasePreflightRunner(ctx, taskplanning.ScriptRunnerInput{
			TaskID: task.ID, WorkItemID: workID, OperationID: operationID, PlanID: planID,
			ExecutionID: ids.NewULID(), SnapshotID: ids.NewULID(), Sources: sources, Preparation: prepared,
		}, operation)
		if err != nil {
			return err
		}
		execution, err := scriptexecutions.NewExecution(scriptexecutions.ExecutionInput{
			TaskID: task.ID, WorkItemID: workID, OperationID: operationID, PlanID: planID,
			ScriptSetGeneration: sources.Script.Record.ScriptSetGeneration, Snapshot: runner.Snapshot,
			Projection: runner.Projection, Run: runner.Run, Body: runner.Body,
		}, task.CreatedAt)
		if err != nil {
			return err
		}
		capture.Preflight = append(capture.Preflight, etcd.GroupReleasePreflightCapture{Sources: sources, Execution: execution})
		capture.Root.Preflight = append(capture.Root.Preflight, releasegroups.PreflightHook{
			WorkItemID: workID, ScriptID: execution.ScriptID, ServiceID: execution.ServiceID, Ordinal: ordinal, Input: execution.Input,
		})
		member.Preparation.Hooks = slices.Delete(member.Preparation.Hooks, index, index+1)
	}
	before := phases.Before[0].When
	kept := make([]releaserender.ReleaseHookRenderInput, 0, len(member.Rollout.Render.Hooks)-len(phases.Before))
	for _, hook := range member.Rollout.Render.Hooks {
		if hook.When != before {
			kept = append(kept, hook)
		}
	}
	member.Rollout.Render.Hooks = kept
	member.Preparation.Rollout, err = releaserender.ServiceRolloutInput(member.Rollout)
	return err
}
