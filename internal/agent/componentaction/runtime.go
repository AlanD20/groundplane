package componentaction

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"io"
	"time"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	"github.com/AlanD20/groundplane/internal/infra/docker/dnsresolverobserver"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelpercontainer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Runtime struct {
	catalog       Catalog
	managedHelper managedConfigExecutor
	composeHelper agent.ComposeHelper
	observer      dnsResolverObserver
}

type dnsResolverObserver interface {
	Observe(context.Context, dnsresolverobserver.Request) (*agentpb.DNSResolverObservationEvidence, error)
}

type managedConfigExecutor interface {
	Validate(context.Context, managedconfighelpercontainer.ValidatorImage, []string, []byte) error
	Execute(context.Context, *agentpb.ManagedConfigHelperRequest) (*agentpb.ManagedConfigHelperResponse, error)
}

func New(
	catalog Catalog,
	managedHelper managedConfigExecutor,
	composeHelper agent.ComposeHelper,
	observer dnsResolverObserver,
) (*Runtime, error) {
	if managedHelper == nil || composeHelper == nil || observer == nil {
		return nil, errs.New(errs.KindInternal, "registered Component action helper is required")
	}
	return &Runtime{
		catalog: catalog, managedHelper: managedHelper, composeHelper: composeHelper, observer: observer,
	}, nil
}

func (runtime *Runtime) ExecuteComponentAction(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	payload agent.ManagedConfigPayload,
) (result *agent.ComponentActionResult, resultErr error) {
	if runtime == nil || runtime.managedHelper == nil || runtime.composeHelper == nil || ctx == nil {
		return nil, closeComponentArtifact(payload.Source, errs.New(
			errs.KindInternal,
			"agent: registered Component action runtime is not configured",
		))
	}
	action := step.GetComponentApply()
	envelope, err := agent.DecodeComponentAction(action)
	if err != nil {
		return nil, closeComponentArtifact(payload.Source, err)
	}
	if assignment.Plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
		if payload.Source != nil {
			return nil, closeComponentArtifact(payload.Source, errs.New(
				errs.KindInternal, "agent: container Component action received managed content",
			))
		}
		_, _, _, resolveErr := runtime.catalog.ResolveContainerConfigActionEnvelope(envelope)
		if resolveErr != nil {
			return nil, resolveErr
		}
		response, executeErr := runtime.composeHelper.Execute(ctx, &agentpb.ComposeHelperRequest{
			Schema:         1,
			AssignmentId:   assignment.AssignmentID,
			TaskId:         assignment.TaskID,
			OperationId:    assignment.OperationID,
			Plan:           assignment.Plan,
			StepId:         step.GetStepId(),
			TimeoutSeconds: step.GetTimeoutSeconds(),
		})
		if executeErr != nil {
			return nil, executeErr
		}
		if response == nil || response.GetSchema() != 1 ||
			response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
			response.GetExitCode() != 0 ||
			response.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
			return nil, errs.New(errs.KindRequestFailed, "agent: Component container action failed")
		}
		return nil, nil
	}
	_, _, observationRecipe, observationErr := runtime.catalog.ResolveDNSResolverObservationActionEnvelope(envelope)
	if observationErr == nil {
		if payload.Source != nil {
			return nil, closeComponentArtifact(payload.Source, errs.New(
				errs.KindValidationFailed,
				"agent: DNS resolver observation received managed content",
			))
		}
		request, requestErr := dnsResolverObservationRequest(assignment, action, observationRecipe)
		if requestErr != nil {
			return nil, requestErr
		}
		evidence, observeErr := runtime.observer.Observe(ctx, request)
		if observeErr != nil {
			return nil, observeErr
		}
		return &agent.ComponentActionResult{DNSResolverObservation: evidence}, nil
	}
	if payload.Source == nil {
		return nil, errs.New(errs.KindInternal, "agent: managed Component action content is missing")
	}
	defer func() {
		if closeErr := payload.Source.Close(); closeErr != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, closeErr))
		}
	}()
	artifact := envelope.Artifact()
	digest := artifact.Digest()
	if payload.Header.ArtifactID != artifact.ID().String() ||
		payload.Header.MediaType != managedconfig.MediaTypeTextUTF8 ||
		payload.Header.Length == 0 || payload.Header.Length > managedconfig.MaximumArtifactBytes ||
		subtle.ConstantTimeCompare(payload.Header.Digest[:], digest[:]) != 1 {
		return nil, errs.New(errs.KindValidationFailed, "agent: managed-config payload does not match Component action")
	}
	content, err := io.ReadAll(io.LimitReader(payload.Source, managedconfig.MaximumArtifactBytes+1))
	if err != nil {
		clear(content)
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(content)
	contentDigest := sha256.Sum256(content)
	if uint64(len(content)) != payload.Header.Length ||
		subtle.ConstantTimeCompare(contentDigest[:], digest[:]) != 1 {
		return nil, errs.New(errs.KindValidationFailed, "agent: managed-config source changed after transfer")
	}
	_, _, recipe, err := runtime.catalog.ResolveManagedConfigActionEnvelope(envelope)
	if err != nil {
		return nil, err
	}
	if err := runtime.managedHelper.Validate(
		ctx, selectedValidatorImage(recipe.Image()), recipe.ValidateArgs(), content,
	); err != nil {
		return nil, err
	}
	request := managedConfigRequest(
		assignment,
		action,
		recipe.RelativePath(),
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH,
	)
	request.Content = content
	result = &agent.ComponentActionResult{}
	state, err := runtime.executeManagedConfig(ctx, request)
	if err != nil {
		return result, err
	}
	result.ManagedConfig = &state
	return result, nil
}

func (runtime *Runtime) FinalizeManagedConfig(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	commit bool,
) (agent.ManagedConfigTransactionState, error) {
	if runtime == nil || runtime.managedHelper == nil || ctx == nil || step == nil {
		return agent.ManagedConfigTransactionState{}, errs.New(
			errs.KindInternal,
			"agent: managed-config finalization runtime is not configured",
		)
	}
	action := step.GetComponentApply()
	envelope, err := agent.DecodeComponentAction(action)
	if err != nil {
		return agent.ManagedConfigTransactionState{}, err
	}
	_, _, recipe, err := runtime.catalog.ResolveManagedConfigActionEnvelope(envelope)
	if err != nil {
		return agent.ManagedConfigTransactionState{}, err
	}
	operation := agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK
	if commit {
		operation = agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_COMMIT
	}
	return runtime.executeManagedConfig(
		ctx,
		managedConfigRequest(assignment, action, recipe.RelativePath(), operation),
	)
}

func (runtime *Runtime) executeManagedConfig(
	ctx context.Context,
	request *agentpb.ManagedConfigHelperRequest,
) (agent.ManagedConfigTransactionState, error) {
	response, firstErr := runtime.managedHelper.Execute(ctx, request)
	state, stateErr := managedConfigTransactionState(request, response)
	if firstErr == nil && stateErr == nil {
		return state, nil
	}
	firstErr = errors.Join(firstErr, stateErr)
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	response, recoveryErr := runtime.managedHelper.Execute(recoveryCtx, request)
	if recoveryErr == nil {
		state, recoveryErr = managedConfigTransactionState(request, response)
	}
	if recoveryErr != nil {
		return agent.ManagedConfigTransactionState{}, errs.Wrap(
			errs.KindRequestFailed,
			errors.Join(firstErr, recoveryErr),
		)
	}
	return state, nil
}

func closeComponentArtifact(source io.ReadCloser, operationErr error) error {
	if source == nil {
		return operationErr
	}
	return errs.Wrap(errs.KindInternal, errors.Join(operationErr, source.Close()))
}
