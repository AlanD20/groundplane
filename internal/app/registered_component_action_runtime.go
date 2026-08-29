package app

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelper"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelpercontainer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type registeredComponentActionRuntime struct {
	catalog       registeredActionCatalog
	managedHelper *managedconfighelpercontainer.Executor
	composeHelper agent.ComposeHelper
}

func newRegisteredComponentActionRuntime(
	catalog registeredActionCatalog,
	managedHelper *managedconfighelpercontainer.Executor,
	composeHelper agent.ComposeHelper,
) (*registeredComponentActionRuntime, error) {
	if managedHelper == nil || composeHelper == nil {
		return nil, errs.New(errs.KindInternal, "registered Component action helper is required")
	}
	return &registeredComponentActionRuntime{
		catalog: catalog, managedHelper: managedHelper, composeHelper: composeHelper,
	}, nil
}

func (runtime *registeredComponentActionRuntime) ExecuteComponentAction(
	ctx context.Context,
	assignment agent.Assignment,
	step *agentpb.ExecutionStep,
	payload agent.ManagedConfigPayload,
) (resultErr error) {
	if runtime == nil || runtime.managedHelper == nil || runtime.composeHelper == nil || ctx == nil {
		return closeComponentArtifact(payload.Source, errs.New(
			errs.KindInternal,
			"agent: registered Component action runtime is not configured",
		))
	}
	action := step.GetComponentApply()
	envelope, err := agent.DecodeComponentAction(action)
	if err != nil {
		return closeComponentArtifact(payload.Source, err)
	}
	if assignment.Plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
		if payload.Source != nil {
			return closeComponentArtifact(payload.Source, errs.New(
				errs.KindInternal, "agent: container Component action received managed content",
			))
		}
		_, _, _, resolveErr := runtime.catalog.ResolveContainerConfigActionEnvelope(envelope)
		if resolveErr != nil {
			return resolveErr
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
			return executeErr
		}
		if response == nil || response.GetSchema() != 1 ||
			response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
			response.GetExitCode() != 0 ||
			response.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
			return errs.New(errs.KindRequestFailed, "agent: Component container action failed")
		}
		return nil
	}
	if payload.Source == nil {
		return errs.New(errs.KindInternal, "agent: managed Component action content is missing")
	}
	defer func() {
		if closeErr := payload.Source.Close(); closeErr != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, closeErr))
		}
	}()
	_, _, recipe, err := runtime.catalog.ResolveManagedConfigActionEnvelope(envelope)
	if err != nil {
		return err
	}
	artifact := envelope.Artifact()
	digest := artifact.Digest()
	if payload.Header.ArtifactID != artifact.ID().String() ||
		payload.Header.MediaType != managedconfig.MediaTypeTextUTF8 ||
		payload.Header.Length == 0 || payload.Header.Length > managedconfig.MaximumArtifactBytes ||
		subtle.ConstantTimeCompare(payload.Header.Digest[:], digest[:]) != 1 {
		return errs.New(errs.KindValidationFailed, "agent: managed-config payload does not match Component action")
	}
	content, err := io.ReadAll(io.LimitReader(payload.Source, managedconfig.MaximumArtifactBytes+1))
	if err != nil {
		clear(content)
		return errs.Wrap(errs.KindInternal, err)
	}
	defer clear(content)
	contentDigest := sha256.Sum256(content)
	if uint64(len(content)) != payload.Header.Length ||
		subtle.ConstantTimeCompare(contentDigest[:], digest[:]) != 1 {
		return errs.New(errs.KindValidationFailed, "agent: managed-config source changed after transfer")
	}
	return runtime.managedHelper.Execute(ctx, &agentpb.ManagedConfigHelperRequest{
		Schema:       managedconfighelper.SchemaVersion,
		ArtifactId:   artifact.ID().String(),
		RelativePath: recipe.RelativePath(),
		Content:      content,
		Sha256:       append([]byte(nil), digest[:]...),
	})
}

func closeComponentArtifact(source io.ReadCloser, operationErr error) error {
	if source == nil {
		return operationErr
	}
	return errs.Wrap(errs.KindInternal, errors.Join(operationErr, source.Close()))
}
