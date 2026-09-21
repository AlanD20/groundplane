package releaseoperation

import (
	"context"
	releasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type preparedReleaseHooks struct {
	task       etcd.TaskRecord
	members    []releaserender.ReleaseTaskRenderMember
	sources    map[string]etcd.ScriptExecutionSources
	executions int
}

func (service *Service) prepareReleaseHooks(
	ctx context.Context,
	scope releasequeries.ReleasePlanningScope,
	revision int64,
	operation domain.OperationKind,
	task etcd.TaskRecord,
	members []releaserender.ReleaseTaskRenderMember,
) (preparedReleaseHooks, error) {
	if service.scripts == nil || service.preparation == nil || revision <= 0 {
		return preparedReleaseHooks{}, errs.New(errs.KindInternal, "release hook dependencies are not configured")
	}
	selectionScope := scope.Clone()
	selectionScope.ReadRevision = revision
	preSteps, postSteps, failureSteps := []taskjournal.TaskStepRecord{}, []taskjournal.TaskStepRecord{}, []taskjournal.TaskStepRecord{}
	sourcesByExecution := make(map[string]etcd.ScriptExecutionSources)
	bodyBytes := uint64(0)
	for memberIndex := range members {
		member := &members[memberIndex]
		scriptIDs, err := service.ledger.ListPlanningHookScriptIDs(
			ctx, selectionScope, member.Render.ServiceID, operation,
		)
		if err != nil {
			return preparedReleaseHooks{}, err
		}
		for _, scriptID := range scriptIDs {
			sources, err := service.scripts.LoadReleaseHookExecutionSources(
				ctx, service.ledger, scriptID, member.Intent.ID, revision,
			)
			if err != nil {
				return preparedReleaseHooks{}, err
			}
			if sources.Script.Record.Desired.When == core.ScriptOnFailure {
				targetReleaseID := domain.FailureHookTargetReleaseID(member.Intent)
				if targetReleaseID != member.Intent.ID {
					sources, err = service.scripts.LoadReleaseHookExecutionSources(
						ctx, service.ledger, scriptID, targetReleaseID, revision,
					)
					if err != nil {
						return preparedReleaseHooks{}, err
					}
				}
			}
			prepared, err := service.preparation.Prepare(ctx, sources)
			if err != nil {
				return preparedReleaseHooks{}, err
			}
			stepID, executionID, snapshotID := ids.New(ids.KindStep), ids.NewULID(), ids.NewULID()
			hook, err := taskplanning.BuildReleaseHookRenderInput(ctx, taskplanning.ManualScriptPlanInput{
				TaskID: task.ID, OperationID: task.OperationID, PlanID: task.PlanID,
				StepID: stepID, ExecutionID: executionID, SnapshotID: snapshotID,
				Sources: sources, Preparation: prepared,
			})
			if err != nil {
				return preparedReleaseHooks{}, err
			}
			member.Render.Hooks = append(member.Render.Hooks, hook)
			sourcesByExecution[executionID] = sources
			bodyBytes += uint64(hook.BodySize)
			task.Params[releaserender.ReleaseHookStepMemberParam(stepID)] = strconv.Itoa(memberIndex + 1)
			task.Params[releaserender.ReleaseHookStepExecutionParam(stepID)] = executionID
			step := taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: stepID}
			switch hook.When {
			case core.ScriptPreDeploy, core.ScriptPreRollback:
				preSteps = append(preSteps, step)
			case core.ScriptPostDeploy, core.ScriptPostRollback:
				postSteps = append(postSteps, step)
			case core.ScriptOnFailure:
				failureSteps = append(failureSteps, step)
			}
		}
	}
	total := len(preSteps) + len(postSteps) + len(failureSteps)
	if total > 16 || bodyBytes > 1<<20 {
		return preparedReleaseHooks{}, errs.New(
			errs.KindValidationFailed,
			"release hook selection exceeds its operation bounds",
		)
	}
	task.Steps = append(task.Steps, preSteps...)
	task.Steps = append(task.Steps, postSteps...)
	task.Steps = append(task.Steps, failureSteps...)
	return preparedReleaseHooks{
		task: task, members: members, sources: sourcesByExecution, executions: total,
	}, nil
}
