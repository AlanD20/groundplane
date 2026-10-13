package blueprintrelease

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// finishSelectedRuntime binds current file recovery to the staged native rollout
// and prepares the one atomic Release publication. Native planning, image
// selection and hook input construction are not repeated in this phase.
func (service *Service) finishSelectedRuntime(
	ctx context.Context,
	input prepareInput,
	captured preparedSelectedRuntime,
	addresses networkreservations.ProxyAddresses,
) (Prepared, error) {
	var err error
	task := captured.task
	runtime := captured.runtime
	hooks, manifest := captured.hooks, captured.manifest
	task, err = service.prepareTaskConfiguration(ctx, task, input.Environment.ReadRevision)
	if err != nil {
		return Prepared{}, err
	}
	task, plan, err := service.plans.SealBlueprintReleaseRuntime(ctx, task, runtime)
	if err != nil {
		return Prepared{}, err
	}
	var rollout *keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]
	if taskjournal.IsBlueprintChild(task.Params) {
		capture, err := runtime.Capture()
		if err != nil {
			return Prepared{}, err
		}
		stored, err := service.ledger.StageBlueprintServiceRollout(ctx, capture)
		clear(capture.NativePlan)
		if err != nil {
			return Prepared{}, err
		}
		defer clear(stored.Record.NativePlan)
		rollout = &stored
	}
	if err := service.ledger.ValidatePostgresPatchPlan(ctx, plan, input.Environment.ReadRevision); err != nil {
		return Prepared{}, err
	}
	candidateDescriptor, err := executionplan.DescribeCandidateRelease(plan)
	if err != nil {
		return Prepared{}, errs.Wrap(errs.KindInternal, err)
	}
	var executions []scriptexecutions.ScriptExecutionRecord
	if len(plan.GetScriptRunnerSnapshots()) != 0 {
		executions, err = etcd.NewScriptExecutionRecords(task, plan, input.CreatedAt)
		if err != nil {
			return Prepared{}, err
		}
	}
	if len(executions) != hooks.executions {
		return Prepared{}, errs.New(errs.KindInternal, "Blueprint post-deploy execution authority is incomplete")
	}
	hookPublications := make([]etcd.ReleaseHookExecutionPublication, len(executions))
	for index, execution := range executions {
		sources, exists := hooks.sources[execution.ID]
		if !exists {
			return Prepared{}, errs.New(errs.KindInternal, "Blueprint post-deploy execution source is missing")
		}
		hookPublications[index] = etcd.ReleaseHookExecutionPublication{Sources: sources, Execution: execution}
	}
	hookPrepared, err := service.ledger.PrepareBlueprintReleaseHooks(ctx, task, hookPublications)
	if err != nil {
		return Prepared{}, err
	}
	for index := range hookPublications {
		hookPublications[index].SnapshotRevision = hookPrepared.SnapshotRevision(
			hookPublications[index].Execution.SnapshotID,
		)
	}
	sourceMembers, err := service.ledger.BlueprintReleaseSourceMembers(ctx, manifest, hookPublications)
	if err != nil {
		return Prepared{}, err
	}
	var sourcePrepared scriptsourcepublication.PreparedSourceSet
	if len(sourceMembers) != 0 {
		sourcePrepared, err = service.sources.Prepare(ctx, task.OperationID, sourceMembers)
		if err != nil {
			if blueprintPublicationOutcomeUnknown(err) {
				return Prepared{}, err
			}
			if cleanupErr := service.sources.Abandon(ctx, task.OperationID, sourceMembers); cleanupErr != nil {
				return Prepared{}, errors.Join(err, cleanupErr)
			}
			return Prepared{}, err
		}
	}
	publication, err := service.ledger.PrepareBlueprintReleasePublication(
		ctx,
		service.sources,
		etcd.BlueprintReleasePublicationEvidence{
			RenderingInput:     input.Projection,
			ServiceRollout:     rollout,
			ProxyAddresses:     addresses,
			NativePredecessors: captured.predecessors,
			Manifest:           manifest, EnvironmentID: input.Environment.Record.ID, Task: task, PublishedAt: input.CreatedAt,
			CandidateReleaseDescriptor: candidateDescriptor,
			Plan:                       plan,
			Hooks:                      hookPublications, HookPrepared: hookPrepared, SourcePrepared: sourcePrepared, SourceMembers: sourceMembers,
		},
	)
	if err != nil {
		if blueprintPublicationOutcomeUnknown(err) {
			return Prepared{}, err
		}
		if len(sourceMembers) != 0 {
			if cleanupErr := service.sources.Abandon(ctx, task.OperationID, sourceMembers); cleanupErr != nil {
				return Prepared{}, errors.Join(err, cleanupErr)
			}
		}
		return Prepared{}, err
	}
	return Prepared{Task: task, Plan: plan, Publication: publication}, nil
}

func blueprintPublicationOutcomeUnknown(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
