package composehelper

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"math"
	"time"
)

// Execute validates one request, runs its closed Compose procedure, and
// returns only a bounded diagnostic category. Raw process output is discarded.
func Execute(
	ctx context.Context,
	taskRunner runner.Runner,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	return execute(ctx, taskRunner, request, nil)
}

// ExecuteWithComponentCatalog additionally enables the closed registered
// Component procedure resolved inside the helper process.
func ExecuteWithComponentCatalog(
	ctx context.Context,
	taskRunner runner.Runner,
	request *agentpb.ComposeHelperRequest,
	catalog ComponentActionCatalog,
) (*agentpb.ComposeHelperResponse, error) {
	if catalog == nil {
		return nil, errs.New(errs.KindInternal, "Compose helper Component catalog is required")
	}
	return execute(ctx, taskRunner, request, catalog)
}

func execute(
	ctx context.Context,
	taskRunner runner.Runner,
	request *agentpb.ComposeHelperRequest,
	catalog ComponentActionCatalog,
) (*agentpb.ComposeHelperResponse, error) {
	if taskRunner == nil {
		return nil, errs.New(errs.KindInternal, "Compose helper Runner is required")
	}
	owned, step, artifact, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	if response, handled, removeErr := executeManagedRemove(ctx, taskRunner, owned, step); handled {
		return response, removeErr
	}
	if ensure := step.GetManagedNetworkEnsure(); ensure != nil {
		return executeManagedNetworkEnsure(ctx, taskRunner, owned.TimeoutSeconds, artifact, ensure)
	}
	if ensure := step.GetManagedVolumeEnsure(); ensure != nil {
		return executeManagedVolumeEnsure(ctx, taskRunner, owned.TimeoutSeconds, artifact, ensure)
	}
	if apply := step.GetComponentApply(); apply != nil {
		if owned.Plan.GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
			return nil, errs.New(
				errs.KindValidationFailed,
				"managed Component action is not a container helper procedure",
			)
		}
		if catalog == nil {
			return nil, errs.New(errs.KindValidationFailed, "Component action has no compiled helper catalog")
		}
		recipe, resolveErr := catalog.ResolveContainerConfigAction(apply)
		if resolveErr != nil {
			return nil, resolveErr
		}
		return executeComponentContainerConfigAction(
			ctx, taskRunner, owned.TimeoutSeconds, artifact, apply, recipe,
		)
	}
	if step.GetServiceProxySwitch() != nil || step.GetServiceProxyProbe() != nil ||
		step.GetServiceProxyCompensate() != nil {
		return executeServiceProxy(ctx, taskRunner, owned.TimeoutSeconds, owned.Plan, artifact, step)
	}
	if step.GetCandidateRestorationProbe() != nil || step.GetCandidateRestorationCompensate() != nil {
		switch executionplan.RestorationTargetForService(owned.GetRestorationAuthority(), restorationStepServiceID(step)) {
		case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR:
			return executeServingPredecessor(ctx, taskRunner, owned.TimeoutSeconds, owned, artifact, step)
		case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE:
			return executeCandidateAbsence(ctx, taskRunner, owned.TimeoutSeconds, owned, artifact, step)
		default:
			return nil, errs.New(errs.KindValidationFailed, "candidate restoration member target is invalid")
		}
	}
	commands, err := commandsFor(owned, step, artifact)
	if err != nil {
		return nil, err
	}
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(owned.TimeoutSeconds)*time.Second)
	defer cancel()
	for index, command := range commands {
		result, runErr := taskRunner.Run(executionCtx, command)
		if contextErr := executionCtx.Err(); contextErr != nil {
			return nil, contextErr
		}
		if result.ExitCode < 0 || result.ExitCode > math.MaxInt32 {
			if runErr == nil {
				runErr = errors.New("process returned an invalid exit code")
			}
			return nil, errs.Wrap(errs.KindInternal, runErr)
		}
		if runErr != nil || result.ExitCode != 0 {
			diagnostic := agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED
			if index == 0 && step.GetComposeApply() != nil {
				diagnostic = agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CONFIG_REJECTED
			}
			return &agentpb.ComposeHelperResponse{
				Schema:   SchemaVersion,
				Outcome:  agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
				ExitCode: int32(result.ExitCode), Diagnostic: diagnostic,
			}, nil
		}
		if index == 0 && needsManagedVolumeEnsure(step, artifact) {
			failure, ensureErr := ensureManagedComposeVolumes(executionCtx, taskRunner, artifact)
			if ensureErr != nil {
				return nil, ensureErr
			}
			if failure != nil {
				return failure, nil
			}
		}
	}
	response := &agentpb.ComposeHelperResponse{
		Schema:     SchemaVersion,
		Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}
	if compensate := step.GetServiceRecreateCompensate(); compensate != nil {
		response.RecreateEvidence = &agentpb.ServiceRecreateEvidence{
			ServiceId:   compensate.ServiceId,
			ReleaseId:   compensate.PriorReleaseId,
			ArtifactId:  compensate.ArtifactId,
			Compensated: true,
			Target:      compensate.PriorTarget,
		}
	}
	return response, nil
}

func completedResponse() *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}
}

func failedResponse(exitCode int32) *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
		ExitCode: exitCode, Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED,
	}
}
