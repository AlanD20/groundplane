package agent

import (
	"context"
	"errors"
	componentaction "github.com/AlanD20/groundplane/internal/agent/componentaction"
	composeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	filematerialization "github.com/AlanD20/groundplane/internal/agent/materialization"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (p *WorkerPool) executeBlueprintReleaseSetup(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) (composeruntime.StepResult, error) {
	switch step.GetPayload().(type) {
	case *agentpb.ExecutionStep_MaterializeFile:
		payload, err := p.materializations.Take(ctx, assignment.TaskID, step.StepId)
		if err != nil {
			return composeruntime.StepResult{}, err
		}
		if p.materializer == nil {
			return composeruntime.StepResult{}, filematerialization.CloseSourceWithError(payload.Source, "agent: materialization runtime is not configured")
		}
		return composeruntime.StepResult{}, p.materializer.ExecuteStep(ctx, assignment, step, payload)
	case *agentpb.ExecutionStep_AdapterProcedure:
		result, err := p.adapter.ExecuteStep(ctx, step)
		return composeruntime.StepResult{ExitCode: result.ExitCode}, err
	case *agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure, *agentpb.ExecutionStep_EnvironmentDirectoryCreate:
		result, err := p.environmentDirectories.ExecuteStep(ctx, assignment, step, p.CheckpointVolumeRemoval)
		return composeruntime.StepResult{ExitCode: result.ExitCode}, err
	default:
		return composeruntime.StepResult{}, errs.New(errs.KindInternal, "agent: unsupported Blueprint Release setup step")
	}
}

func (p *WorkerPool) executeBlueprintReleaseComponent(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) (composeruntime.StepResult, error) {
	action := step.GetComponentApply()
	if action == nil || action.ManagedConfigContent || len(action.ExpectedPreviousArtifactDigest) != 0 ||
		action.ExpectedPreviousArtifactId != "" || action.ExpectedPreviousGeneration != 0 ||
		assignment.Plan.GetComponentLifecycleMode() != agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UNSPECIFIED {
		return composeruntime.StepResult{}, errs.New(
			errs.KindInternal,
			"agent: Blueprint Component action contains host lifecycle authority",
		)
	}
	if p.componentActions == nil {
		return composeruntime.StepResult{}, errs.New(errs.KindInternal, "agent: Component action runtime is not configured")
	}
	result := composeruntime.StepResult{MutationAttempted: true}
	observation, err := p.componentActions.ExecuteComponentAction(ctx, assignment, step, componentaction.ManagedConfigPayload{})
	if observation != nil && (observation.ManagedConfig != nil || observation.DNSResolverObservation != nil) {
		err = errors.Join(
			err,
			errs.New(errs.KindInternal, "agent: Blueprint Component action returned host lifecycle evidence"),
		)
	}
	result.ReconciliationRequired = err != nil
	return result, err
}
