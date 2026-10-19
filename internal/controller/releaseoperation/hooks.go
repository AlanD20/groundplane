package releaseoperation

import (
	"context"
	releasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	scriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type preparedReleaseHooks struct {
	members    []releaserender.ReleaseTaskRenderMember
	sources    map[string]scriptsourcequeries.ScriptExecutionSources
	executions int
}

func (service *Service) prepareReleaseHooks(
	ctx context.Context,
	scope releasequeries.ReleasePlanningScope,
	revision int64,
	operation domain.OperationKind,
	work workqueue.View,
	operationID, planID string,
	members []releaserender.ReleaseTaskRenderMember,
) (preparedReleaseHooks, error) {
	if service.scripts == nil || service.preparation == nil || revision <= 0 {
		return preparedReleaseHooks{}, errs.New(errs.KindInternal, "release hook dependencies are not configured")
	}
	task, item := work.Task.Task, work.Item.Item
	if task.Validate() != nil || item.Validate() != nil || item.TaskID != task.ID ||
		item.Operation != workexecution.Release || item.Placement.Executor != workexecution.Agent ||
		ids.Validate(ids.KindOperation, operationID) != nil || ids.Validate(ids.KindPlan, planID) != nil ||
		task.Scope.EnvironmentID != scope.Environment.Record.ID ||
		task.Scope.ProjectID != scope.Project.Record.ID || task.Scope.TenantID != scope.Project.Record.TenantID ||
		item.AttemptNumber != 0 || item.ForwardRevoked || task.Stop != nil || task.FinishedAt != nil ||
		(item.Phase != workexecution.Waiting && item.Phase != workexecution.Ready) ||
		len(members) != 1 || members[0].Render.ServiceID != item.TargetID {
		return preparedReleaseHooks{}, errs.New(
			errs.KindValidationFailed,
			"release hooks require unstarted native Service work",
		)
	}
	selectionScope := scope.Clone()
	selectionScope.ReadRevision = revision
	sourcesByExecution := make(map[string]scriptsourcequeries.ScriptExecutionSources)
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
				ctx, service.ledger.Reader, scriptID, member.Intent.ID, revision,
			)
			if err != nil {
				return preparedReleaseHooks{}, err
			}
			if sources.Script.Record.Desired.When == core.ScriptOnFailure {
				targetReleaseID := domain.FailureHookTargetReleaseID(member.Intent)
				if targetReleaseID != member.Intent.ID {
					sources, err = service.scripts.LoadReleaseHookExecutionSources(
						ctx, service.ledger.Reader, scriptID, targetReleaseID, revision,
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
			executionID, snapshotID := ids.NewULID(), ids.NewULID()
			hook, err := taskplanning.BuildReleaseHookRenderInput(ctx, taskplanning.ScriptRunnerInput{
				TaskID: task.ID, WorkItemID: item.ID, OperationID: operationID, PlanID: planID,
				ExecutionID: executionID, SnapshotID: snapshotID,
				Sources: sources, Preparation: prepared,
			})
			if err != nil {
				return preparedReleaseHooks{}, err
			}
			member.Render.Hooks = append(member.Render.Hooks, hook)
			sourcesByExecution[executionID] = sources
			bodyBytes += uint64(hook.BodySize)
		}
	}
	total := len(sourcesByExecution)
	if total > 16 || bodyBytes > 1<<20 {
		return preparedReleaseHooks{}, errs.New(
			errs.KindValidationFailed,
			"release hook selection exceeds its operation bounds",
		)
	}
	return preparedReleaseHooks{
		members: members, sources: sourcesByExecution, executions: total,
	}, nil
}
