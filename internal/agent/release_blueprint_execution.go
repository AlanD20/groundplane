package agent

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (p *WorkerPool) executeBlueprintReleaseSetup(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
) (composeStepResult, error) {
	switch step.GetPayload().(type) {
	case *agentpb.ExecutionStep_MaterializeFile:
		payload, err := p.materializations.Take(ctx, assignment.TaskID, step.StepId)
		if err != nil {
			return composeStepResult{}, err
		}
		if p.materializer == nil {
			return composeStepResult{}, closeMaterializationSource(payload.Source, "agent: materialization runtime is not configured")
		}
		return composeStepResult{}, p.materializer.executeStep(ctx, assignment, step, payload)
	case *agentpb.ExecutionStep_AdapterProcedure:
		result, err := p.adapter.ExecuteStep(ctx, step)
		return composeStepResult{ExitCode: result.ExitCode}, err
	case *agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure, *agentpb.ExecutionStep_EnvironmentDirectoryCreate:
		result, err := p.environmentDirectories.executeStep(ctx, assignment, step, p.CheckpointVolumeRemoval)
		return composeStepResult{ExitCode: result.ExitCode}, err
	default:
		return composeStepResult{}, errs.New(errs.KindInternal, "agent: unsupported Blueprint Release setup step")
	}
}

func (p *WorkerPool) executeBlueprintReleaseComponent(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
) (composeStepResult, error) {
	action := step.GetComponentApply()
	if action == nil || action.ManagedConfigContent || len(action.ExpectedPreviousArtifactDigest) != 0 ||
		action.ExpectedPreviousArtifactId != "" || action.ExpectedPreviousGeneration != 0 ||
		assignment.Plan.GetComponentLifecycleMode() != agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UNSPECIFIED {
		return composeStepResult{}, errs.New(
			errs.KindInternal,
			"agent: Blueprint Component action contains host lifecycle authority",
		)
	}
	if p.componentActions == nil {
		return composeStepResult{}, errs.New(errs.KindInternal, "agent: Component action runtime is not configured")
	}
	result := composeStepResult{MutationAttempted: true}
	observation, err := p.componentActions.ExecuteComponentAction(ctx, assignment, step, ManagedConfigPayload{})
	if observation != nil && (observation.ManagedConfig != nil || observation.DNSResolverObservation != nil) {
		err = errors.Join(
			err,
			errs.New(errs.KindInternal, "agent: Blueprint Component action returned host lifecycle evidence"),
		)
	}
	result.ReconciliationRequired = err != nil
	return result, err
}
